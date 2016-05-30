"""Thrift integration for Baseplate.

This module provides an implementation of :py:class:`TProcessor` which
integrates Baseplate's facilities into the Thrift request lifecycle.

An abbreviated example of it in use::

    example = import_thriftfile(__name__, "example.thrift")

    def make_processor(app_config):
        baseplate = Baseplate()
        handler = MyHandler()
        return TBaseplateProcessor(logger, baseplate, example.Service, handler)

TODO:

- lots of code cleanup
  - baseplate.integration.thrift.TBaseplateProcessor
  - baseplate.thrift_pool._make_protocol
  - baseplate.context.thrift.*
- fix the tests, get some integration testing?
- make the client work
- test against real finagle
- test against theaderprotocol r2 clients for upgrade path
- travis

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals


from thriftpy.thrift import (
    TApplicationException,
    TException,
    TMessageType,
    TType,
)

from ..core import TraceInfo
from ..thrift_pool import import_thriftfile


baseplate_thrift = import_thriftfile("baseplate", "thrift/tracing.thrift")


class RequestContext(object):
    pass


class TBaseplateProcessor(object):
    def __init__(self, logger, baseplate, service, handler):
        self.logger = logger
        self.baseplate = baseplate
        self.service = service
        self.handler = handler
        self.is_upgraded = False

    def process(self, iprot, oprot):
        if self.is_upgraded:
            header = baseplate_thrift.RequestHeader()
            header.read(iprot)
        else:
            header = None

        name, message_type, sequence_id = iprot.read_message_begin()
        assert message_type in (TMessageType.CALL, TMessageType.ONEWAY)
        if name == "__can__finagle__trace__v3__":
            args = baseplate_thrift.ConnectionOptions()
            args.read(iprot)
            handler = self._handle_upgrade
        elif name in self.service.thrift_services:
            args_cls = getattr(self.service, "{}_args".format(name))
            args = args_cls()
            args.read(iprot)
            handler = self._handle_method_call
        else:
            iprot.skip(TType.STRUCT)
            handler = self._handle_not_found
        iprot.read_message_end()

        try:
            result = handler(name, header, args)
        except Exception as exc:
            oprot.write_message_begin(name, TMessageType.EXCEPTION, sequence_id)
            if not isinstance(exc, TException):
                exc = TApplicationException(type=TApplicationException.INTERNAL_ERROR,
                                            message=str(exc))
            exc.write(oprot)
            oprot.write_message_end()
            oprot.trans.flush()
        else:
            if not result.oneway:
                oprot.write_message_begin(name, TMessageType.REPLY, sequence_id)
                result.write(oprot)
                oprot.write_message_end()
                oprot.trans.flush()

    def _handle_upgrade(self, name, header, args):
        self.is_upgraded = True
        return baseplate_thrift.UpgradeReply()

    def _handle_not_found(self, name, header, args):
        raise TApplicationException(TApplicationException.UNKNOWN_METHOD)

    def _handle_method_call(self, name, header, args):
        result_cls = getattr(self.service, "{}_result".format(name))
        result = result_cls()

        arg_indexes = sorted(args.thrift_spec.keys())
        arg_names = [args.thrift_spec[i][1] for i in arg_indexes]
        ordered_args = [args.__dict__[arg_name] for arg_name in arg_names]

        trace_info = None
        if header:
            try:
                trace_info = TraceInfo.from_upstream(
                    trace_id=header.trace_id,
                    parent_id=header.parent_span_id,
                    span_id=header.span_id,
                )
            except ValueError:
                pass

        context = RequestContext()
        root_span = self.baseplate.make_root_span(
            context=context,
            name=name,
            trace_info=trace_info,
        )

        handler_method = getattr(self.handler, name)
        try:
            self.logger.debug("Handling: %r", name)
            with root_span:
                result.success = handler_method(context, *ordered_args)
        except Exception as exc:
            for index, spec in result.thrift_spec.items():
                if spec[1] == "success":
                    continue

                _, exc_name, exc_cls, _ = spec
                if isinstance(exc, exc_cls):
                    setattr(result, exc_name, exc)
                    break
            else:
                raise

        return result
