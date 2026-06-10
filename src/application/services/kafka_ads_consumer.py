import logging
import typing
import uuid

from aiokafka import AIOKafkaConsumer

from src.application.ports.usecases import IndexAdPort, RemoveAdPort
from src.tracing import trace_id_var

logger = logging.getLogger(__name__)


class KafkaAdsConsumer:
    def __init__(
        self,
        consumer: AIOKafkaConsumer,
        index_ad: IndexAdPort,
        remove_ad: RemoveAdPort,
    ) -> None:
        self._consumer = consumer
        self._index_ad = index_ad
        self._remove_ad = remove_ad

    async def run(self) -> None:
        async for msg in self._consumer:
            trace_id = self._extract_trace_id(msg.headers)
            token = trace_id_var.set(trace_id)
            try:
                try:
                    await self._handle(msg.value)
                except Exception:
                    logger.exception("failed to handle message %s", msg)
                    continue
                await self._consumer.commit()
            finally:
                trace_id_var.reset(token)

    @staticmethod
    def _extract_trace_id(headers: list[tuple[str, bytes]]) -> str:
        for key, value in headers:
            if key.lower() == "x-trace-id":
                return value.decode("utf-8")
        return str(uuid.uuid4())

    async def _handle(self, value: dict[str, typing.Any]) -> None:
        event = value.get("event")
        payload = value.get("payload") or {}
        ad_id = payload.get("ad_id")
        if not isinstance(ad_id, int):
            logger.warning("skip message without ad_id: %s", value)
            return

        if event in ("ad.created", "ad.updated"):
            await self._index_ad.execute(ad_id)
        elif event == "ad.deleted":
            await self._remove_ad.execute(ad_id)
        else:
            logger.warning("unknown event type: %s", event)
