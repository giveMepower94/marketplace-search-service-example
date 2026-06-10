import uuid

from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Receive, Scope, Send

from src.tracing import trace_id_var

_HEADER = b"x-trace-id"


class TraceIdMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] not in ("http", "websocket"):
            await self.app(scope, receive, send)
            return

        headers = dict(scope["headers"])
        trace_id = headers.get(_HEADER, b"").decode() or str(uuid.uuid4())
        token = trace_id_var.set(trace_id)

        async def send_with_header(message: dict) -> None:
            if message["type"] == "http.response.start":
                mutable = MutableHeaders(scope=message)
                mutable.append("x-trace-id", trace_id)
            await send(message)

        try:
            await self.app(scope, receive, send_with_header)
        finally:
            trace_id_var.reset(token)
