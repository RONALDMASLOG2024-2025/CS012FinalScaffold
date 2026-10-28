import logging
import uuid
from contextvars import ContextVar

# One trace id follows a request from the gateway through every node it touches.
trace_id_var: ContextVar[str] = ContextVar("trace_id", default="NO_TRACE_ID")


class TraceIdFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "trace_id"):
            record.trace_id = trace_id_var.get()
        return True


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] [Trace-ID: %(trace_id)s] %(message)s",
)

for _handler in logging.getLogger().handlers:
    _handler.addFilter(TraceIdFilter())


class TraceLoggerAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        extra = dict(kwargs.get("extra") or {})
        extra["trace_id"] = trace_id_var.get()
        kwargs["extra"] = extra
        return msg, kwargs


def get_logger(name: str):
    logger = logging.getLogger(name)
    return TraceLoggerAdapter(logger, {})


def generate_trace_id() -> str:
    return str(uuid.uuid4())[:8]
