from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import signal

from gevent.pool import Pool
from gevent.server import StreamServer

from thriftpy.transport import (
    TBufferedTransportFactory,
    TSocket,
    TTransportException,
)
from thriftpy.protocol import TBinaryProtocolFactory


# pylint: disable=too-many-public-methods
class GeventServer(StreamServer):
    def __init__(self, processor, *args, **kwargs):
        self.processor = processor
        self.transport_factory = TBufferedTransportFactory()
        self.protocol_factory = TBinaryProtocolFactory()
        super(GeventServer, self).__init__(*args, **kwargs)

    def serve_forever(self, stop_timeout=None):
        signal.signal(signal.SIGINT, lambda sig, frame: self.stop())
        signal.signal(signal.SIGTERM, lambda sig, frame: self.stop())
        super(GeventServer, self).serve_forever(stop_timeout=stop_timeout)

    # pylint: disable=method-hidden
    def handle(self, client_socket, _):
        client = TSocket(sock=client_socket)

        itrans = self.transport_factory.get_transport(client)
        iprot = self.protocol_factory.get_protocol(itrans)

        otrans = self.transport_factory.get_transport(client)
        oprot = self.protocol_factory.get_protocol(otrans)

        try:
            while self.started:
                self.processor.process(iprot, oprot)
        except TTransportException:
            pass
        finally:
            itrans.close()
            otrans.close()


def make_server(config, listener, app):
    max_concurrency = int(config.get("max_concurrency", 0)) or None
    stop_timeout = int(config.get("stop_timeout", 0))

    pool = Pool(size=max_concurrency)
    server = GeventServer(
        processor=app,
        listener=listener,
        spawn=pool,
    )
    server.stop_timeout = stop_timeout
    return server
