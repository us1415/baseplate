"""Thrift integration for Baseplate.

This module provides an implementation of :py:class:`TProcessorEventHandler`
which integrates Baseplate's facilities into the Thrift request lifecycle.

An abbreviated example of it in use::

    def make_processor(app_config):
        baseplate = Baseplate()

        handler = MyHandler()
        processor = my_thrift.MyService.ContextProcessor(handler)

        event_handler = BaseplateProcessorEventHandler(logger, baseplate)
        processor.setEventHandler(event_handler)

        return processor

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from thrift.Thrift import TProcessorEventHandler

from ...core import TraceInfo


class RequestContext(object):
    pass


# TODO: exceptions in the event handler cause the connection to be abruptly
# closed with no diagnostics sent to the client. that should be more obvious.
class BaseplateProcessorEventHandler(TProcessorEventHandler):
    """Processor event handler for Baseplate.

    :param logging.Logger logger: The logger to use for error and debug logging.
    :param baseplate.core.Baseplate baseplate: The baseplate instance for your
        application.

    """
    def __init__(self, logger, baseplate):
        self.logger = logger
        self.baseplate = baseplate

    def getHandlerContext(self, fn_name, server_context):
        context = RequestContext()
        return context

    def postRead(self, handler_context, fn_name, args):
        self.logger.debug("Handling: %r", fn_name)

    def handlerDone(self, handler_context, fn_name, result):
        pass

    def handlerError(self, handler_context, fn_name, exception):
        self.logger.exception("Unexpected exception in %r.", fn_name)
