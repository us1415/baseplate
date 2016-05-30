"""A Thrift client connection pool.

.. note:: See :py:class:`baseplate.context.thrift.ThriftContextFactory` for
    a convenient way to integrate the pool with your application.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import contextlib
import logging
import os
import pkg_resources
import posixpath
import socket
import time

import thriftpy
from thriftpy.transport import TSocket, TTransportException, TBufferedTransportFactory
from thriftpy.protocol import TBinaryProtocol
from thriftpy.protocol.exc import TProtocolException
from thriftpy.thrift import TApplicationException

from ._compat import queue
from .retry import RetryPolicy


logger = logging.getLogger(__name__)


def import_thriftfile(package_name, resource_name):
    thriftfile_path = pkg_resources.resource_filename(package_name, resource_name)

    package_components = [package_name]
    subdirectories, thriftfile_name = posixpath.split(resource_name)
    package_components.extend(filter(None, subdirectories.split(posixpath.sep)))
    module_basename, extension = posixpath.splitext(thriftfile_name)
    assert extension == ".thrift"
    package_components.append(module_basename + "_thrift")
    module_path = ".".join(package_components)

    baseplate_thriftfile = pkg_resources.resource_filename(
        "baseplate", "thrift/baseplate.thrift")
    include_dir = os.path.dirname(baseplate_thriftfile)
    include_dirs = [include_dir.encode()]

    # TODO: cleaner way to deal with encoding here?
    return thriftpy.load(thriftfile_path.encode(), module_path.encode(), include_dirs)


def _make_protocol(endpoint, timeout):
    kwargs = {
        "socket_timeout": timeout * 1000.,
        "connect_timeout": timeout * 1000.,
    }

    if endpoint.family == socket.AF_INET:
        trans = TSocket(*endpoint.address, **kwargs)
    elif endpoint.family == socket.AF_UNIX:
        trans = TSocket(unix_socket=endpoint.address, **kwargs)
    else:
        raise Exception("unsupported endpoint family %r" % endpoint.family)
    buffered_trans = TBufferedTransportFactory().get_transport(trans)
    return TBinaryProtocol(buffered_trans)


class ThriftConnectionPool(object):
    """A pool that maintains a queue of open Thrift connections.

    :param baseplate.config.EndpointConfiguration endpoint: The remote address
        of the Thrift service.
    :param int size: The maximum number of connections that can be open
        before new attempts to open block.
    :param int max_age: The maximum number of seconds a connection should be
        kept alive. Connections older than this will be reaped.
    :param int timeout: The maximum number of seconds a connection attempt or
        RPC call can take before a TimeoutError is raised.
    :param int max_retries: The maximum number of times the pool will attempt
        to open a connection.

    All exceptions raised by this class derive from
    :py:exc:`~thrift.transport.TTransport.TTransportException`.

    """
    # pylint: disable=too-many-arguments
    def __init__(self, endpoint, size=10, max_age=120, timeout=1, max_retries=3):
        self.endpoint = endpoint
        self.max_age = max_age
        self.retry_policy = RetryPolicy.new(attempts=max_retries)
        self.timeout = timeout

        self.pool = queue.LifoQueue()
        for _ in range(size):
            self.pool.put(None)

    def _acquire(self):
        try:
            prot = self.pool.get(block=True, timeout=self.timeout)
        except queue.Empty:
            raise TTransportException(
                type=TTransportException.NOT_OPEN,
                message="timed out waiting for a connection slot",
            )

        for _ in self.retry_policy:
            if prot:
                if time.time() - prot.baseplate_birthdate < self.max_age:
                    return prot
                else:
                    prot.trans.close()
                    prot = None

            prot = _make_protocol(self.endpoint, timeout=self.timeout)

            try:
                prot.trans.open()
            except TTransportException as exc:
                logger.info("Failed to connect to %r: %s",
                    self.endpoint, exc)
                prot = None
                continue

            prot.baseplate_birthdate = time.time()

            return prot

        raise TTransportException(
            type=TTransportException.NOT_OPEN,
            message="giving up after multiple attempts to connect",
        )

    def _release(self, prot):
        if prot.trans.isOpen():
            self.pool.put(prot)
        else:
            self.pool.put(None)

    @contextlib.contextmanager
    def connection(self):
        """Acquire a connection from the pool.

        This method is to be used with a context manager. It returns a
        connection from the pool, or blocks up to :attr:`timeout` seconds
        waiting for one if the pool is full and all connections are in use.

        When the context is exited, the connection is returned to the pool.
        However, if it was exited via an unexpected Thrift exception, the
        connection is closed instead because the state of the connection is
        unknown.

        """
        prot = self._acquire()
        try:
            yield prot
        except (TApplicationException, TProtocolException, TTransportException):
            # these exceptions usually indicate something low-level went wrong,
            # so it's safest to just close this connection because we don't
            # know what state it's in. the only other TException-derived errors
            # should be application level errors which should be safe for the
            # connection.
            prot.trans.close()
            raise
        except socket.timeout:
            # thrift doesn't re-wrap socket timeout errors appropriately so
            # we'll do it here for a saner exception hierarchy
            prot.trans.close()
            raise TTransportException(
                type=TTransportException.TIMED_OUT,
                message="timed out interacting with socket",
            )
        except socket.error as exc:
            prot.trans.close()
            raise TTransportException(
                type=TTransportException.UNKNOWN,
                message=str(exc),
            )
        finally:
            self._release(prot)
