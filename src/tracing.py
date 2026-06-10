import logging
from contextvars import ContextVar

trace_id_var: ContextVar[str] = ContextVar("trace_id", default="n/a")


class TraceIdFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.trace_id = trace_id_var.get()  # type: ignore[attr-defined]
        return True


def configure_logging() -> None:
    root = logging.getLogger()
    if any(isinstance(f, TraceIdFilter) for h in root.handlers for f in h.filters):
        return
    handler = logging.StreamHandler()
    handler.addFilter(TraceIdFilter())
    fmt = "%(asctime)s %(levelname)s [%(trace_id)s] %(name)s: %(message)s"
    handler.setFormatter(logging.Formatter(fmt))
    root.setLevel(logging.INFO)
    root.addHandler(handler)
