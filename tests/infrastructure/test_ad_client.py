import httpx
import pytest

from src.infrastructure.http.ad_client import AdServiceAdSource
from src.tracing import trace_id_var


@pytest.mark.asyncio
async def test_ad_client_sends_trace_id_header() -> None:
    captured: dict[str, httpx.Request] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["request"] = request
        return httpx.Response(404)

    transport = httpx.MockTransport(handler)
    token = trace_id_var.set("trace-abc-123")
    try:
        async with httpx.AsyncClient(transport=transport) as client:
            ad_source = AdServiceAdSource(client, "http://ad-service/")
            await ad_source.get(1)
    finally:
        trace_id_var.reset(token)

    assert captured["request"].headers["x-trace-id"] == "trace-abc-123"
