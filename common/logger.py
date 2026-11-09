import logging
import uuid
from contextvars import ContextVar

# One trace id follows a request from the gateway through every node it touches.
trace_id_var: ContextVar[str] = ContextVar("trace_id", default="NO_TRACE_ID")
request_id_var: ContextVar[str] = ContextVar("NO_REQUEST_ID", default="NO_REQUEST_ID")
span_id_var: ContextVar[str] = ContextVar("NO_SPAN_ID", default="NO_SPAN_ID")
parent_span_id_var: ContextVar[str] = ContextVar("NO_PARENT_SPAN_ID", default="NO_PARENT_SPAN_ID")
generation_var: ContextVar[int] = ContextVar("simulation_generation", default=0)


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

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)


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
    return uuid.uuid4().hex[:16]
