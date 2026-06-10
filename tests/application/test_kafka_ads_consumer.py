import uuid
from dataclasses import dataclass, field
from typing import Any

import pytest

from src.application.ports.usecases import IndexAdPort, RemoveAdPort
from src.application.services.kafka_ads_consumer import KafkaAdsConsumer
from src.tracing import trace_id_var


@dataclass
class FakeMessage:
    value: dict[str, Any]
    headers: list[tuple[str, bytes]]


class RecordingIndexAd(IndexAdPort):
    def __init__(self) -> None:
        self.calls: list[int] = []
        self.trace_ids: list[str] = []

    async def execute(self, ad_id: int) -> None:
        self.calls.append(ad_id)
        self.trace_ids.append(trace_id_var.get())


class RecordingRemoveAd(RemoveAdPort):
    def __init__(self) -> None:
        self.calls: list[int] = []

    async def execute(self, ad_id: int) -> None:
        self.calls.append(ad_id)


@dataclass
class FakeConsumer:
    messages: list[FakeMessage]
    committed: int = 0
    _index: int = field(default=0, init=False)

    def __aiter__(self) -> "FakeConsumer":
        return self

    async def __anext__(self) -> FakeMessage:
        if self._index >= len(self.messages):
            raise StopAsyncIteration
        msg = self.messages[self._index]
        self._index += 1
        return msg

    async def commit(self) -> None:
        self.committed += 1


@pytest.mark.asyncio
async def test_consumer_uses_trace_id_from_headers() -> None:
    messages = [
        FakeMessage(
            value={"event": "ad.created", "payload": {"ad_id": 1}},
            headers=[("X-Trace-Id", b"trace-from-header")],
        )
    ]
    index_ad = RecordingIndexAd()
    consumer = KafkaAdsConsumer(
        consumer=FakeConsumer(messages),
        index_ad=index_ad,
        remove_ad=RecordingRemoveAd(),
    )

    await consumer.run()

    assert index_ad.calls == [1]
    assert index_ad.trace_ids == ["trace-from-header"]
    assert trace_id_var.get() == "n/a"


@pytest.mark.asyncio
async def test_consumer_generates_trace_id_when_missing() -> None:
    messages = [
        FakeMessage(
            value={"event": "ad.created", "payload": {"ad_id": 2}},
            headers=[],
        )
    ]
    index_ad = RecordingIndexAd()
    consumer = KafkaAdsConsumer(
        consumer=FakeConsumer(messages),
        index_ad=index_ad,
        remove_ad=RecordingRemoveAd(),
    )

    await consumer.run()

    assert index_ad.calls == [2]
    generated = index_ad.trace_ids[0]
    assert str(uuid.UUID(generated)) == generated
    assert trace_id_var.get() == "n/a"
