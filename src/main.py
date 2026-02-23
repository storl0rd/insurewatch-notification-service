import os
import logging
import time
import random
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from motor.motor_asyncio import AsyncIOMotorClient
from opentelemetry import trace, metrics
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.logging import LoggingInstrumentor

# ── OTel setup ────────────────────────────────────────────────────────────────
OTLP_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318")
resource = Resource.create({
    "service.name": "notification-service",
    "service.version": "1.0.0",
    "service.language": "python",
    "deployment.environment": os.getenv("ENVIRONMENT", "production"),
})
tracer_provider = TracerProvider(resource=resource)
tracer_provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=f"{OTLP_ENDPOINT}/v1/traces")))
trace.set_tracer_provider(tracer_provider)

meter_provider = MeterProvider(resource=resource, metric_readers=[
    PeriodicExportingMetricReader(OTLPMetricExporter(endpoint=f"{OTLP_ENDPOINT}/v1/metrics"), export_interval_millis=10000)
])
metrics.set_meter_provider(meter_provider)

LoggingInstrumentor().instrument(set_logging_format=True)
logging.basicConfig(level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] [traceId=%(otelTraceID)s spanId=%(otelSpanID)s] %(message)s")
logger = logging.getLogger("notification-service")

# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(title="InsureWatch Notification Service", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
FastAPIInstrumentor.instrument_app(app)

MONGO_URL = os.getenv("MONGODB_URL", "mongodb://localhost:27017")
mongo_client = AsyncIOMotorClient(MONGO_URL)
db = mongo_client.insurewatch
notifications_col = db.notifications

tracer = trace.get_tracer("notification-service", "1.0.0")
meter  = metrics.get_meter("notification-service", "1.0.0")
notifications_sent   = meter.create_counter("notifications.sent.total",   description="Notifications sent")
notifications_failed = meter.create_counter("notifications.failed.total", description="Notifications failed")

chaos_state = {
    "service_crash": False,
    "high_latency":  False,
    "db_failure":    False,
    "memory_spike":  False,
    "cpu_spike":     False,
}
_memory_hog = []

def apply_chaos():
    if chaos_state["service_crash"]:
        raise HTTPException(status_code=503, detail="Service unavailable (chaos: service_crash)")
    if chaos_state["high_latency"]:
        delay = random.uniform(3.0, 8.0)
        logger.warning(f"Chaos: injecting {delay:.1f}s latency")
        time.sleep(delay)
    if chaos_state["db_failure"]:
        raise HTTPException(status_code=503, detail="Database connection failed (chaos: db_failure)")
    if chaos_state["memory_spike"]:
        logger.warning("Chaos: memory spike")
        _memory_hog.append("x" * 50 * 1024 * 1024)
    if chaos_state["cpu_spike"]:
        end = time.time() + 2
        while time.time() < end:
            _ = sum(i * i for i in range(10000))

class NotificationRequest(BaseModel):
    customer_id: str
    event: str
    claim_id: Optional[str] = None
    status: Optional[str] = None
    message: Optional[str] = None

@app.get("/health")
async def health():
    return {"status": "ok", "service": "notification-service", "chaos": chaos_state}

@app.post("/notify")
async def send_notification(req: NotificationRequest):
    with tracer.start_as_current_span("send_notification") as span:
        apply_chaos()
        span.set_attribute("notification.event",       req.event)
        span.set_attribute("notification.customer_id", req.customer_id)

        templates = {
            "claim_submitted": f"Your claim {req.claim_id} has been submitted with status: {req.status}",
            "claim_approved":  f"Great news! Your claim {req.claim_id} has been approved.",
            "claim_rejected":  f"Your claim {req.claim_id} was not approved. Please contact support.",
            "policy_renewed":  "Your policy has been successfully renewed.",
        }
        message = req.message or templates.get(req.event, f"Update on your account: {req.event}")

        doc = {
            "customer_id":  req.customer_id,
            "event":        req.event,
            "claim_id":     req.claim_id,
            "message":      message,
            "status":       "sent",
            "channel":      "email",
            "sent_at":      datetime.utcnow().isoformat(),
        }
        await notifications_col.insert_one(doc)
        notifications_sent.add(1, {"event": req.event, "channel": "email"})
        logger.info(f"Notification sent: {req.event} for customer {req.customer_id}")
        return {"status": "sent", "message": message}

@app.get("/notifications/{customer_id}")
async def get_notifications(customer_id: str):
    with tracer.start_as_current_span("get_notifications") as span:
        apply_chaos()
        span.set_attribute("customer.id", customer_id)
        cursor = notifications_col.find({"customer_id": customer_id}).sort("sent_at", -1).limit(20)
        results = []
        async for doc in cursor:
            doc["_id"] = str(doc["_id"])
            results.append(doc)
        return results

@app.get("/chaos/state")
async def get_chaos_state():
    return chaos_state

@app.post("/chaos/set")
async def set_chaos(state: dict):
    global _memory_hog
    for k, v in state.items():
        if k in chaos_state:
            chaos_state[k] = v
            logger.warning(f"Chaos state: {k}={v}")
    if not chaos_state["memory_spike"]:
        _memory_hog = []
    return {"status": "updated", "chaos": chaos_state}
