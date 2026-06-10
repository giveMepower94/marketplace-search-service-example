import uuid

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from src.presentation.middleware import TraceIdMiddleware


@pytest.fixture
def app_with_middleware() -> FastAPI:
    app = FastAPI()
    app.add_middleware(TraceIdMiddleware)

    @app.get("/ping")
    async def ping() -> dict:
        return {"ok": True}

    return app


@pytest.fixture
async def client(app_with_middleware: FastAPI) -> AsyncClient:
    transport = ASGITransport(app=app_with_middleware)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


@pytest.mark.asyncio
async def test_middleware_returns_provided_trace_id(client: AsyncClient) -> None:
    response = await client.get("/ping", headers={"X-Trace-Id": "my-trace-123"})
    assert response.headers["x-trace-id"] == "my-trace-123"


@pytest.mark.asyncio
async def test_middleware_generates_uuid_when_no_header(client: AsyncClient) -> None:
    response = await client.get("/ping")
    trace_id = response.headers["x-trace-id"]
    parsed = uuid.UUID(trace_id)
    assert str(parsed) == trace_id
