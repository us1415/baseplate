from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from ..core import BaseplateObserver, ServerSpanObserver


class SentryBaseplateObserver(BaseplateObserver):
    def __init__(self, raven):
        self.raven = raven

    def on_server_span_created(self, context, server_span):
        observer = SentryServerSpanObserver(self.raven)
        server_span.register(observer)
        context.raven = self.raven


class SentryServerSpanObserver(ServerSpanObserver):
    def __init__(self, raven):
        self.raven = raven

    def on_start(self):
        self.raven.context.activate()

    def on_set_tag(self, key, value):
        if key.startswith("http"):
            self.raven.http_context({key[len("http."):]: value})
        else:
            self.raven.tags_context({key: value})

    def on_log(self, name, payload):
        self.raven.captureBreadcrumb(category=name, data=payload)

    def on_finish(self, exc_info=None):
        if exc_info is not None:
            self.raven.captureException(exc_info=exc_info)
        self.raven.context.clear(deactivate=True)
