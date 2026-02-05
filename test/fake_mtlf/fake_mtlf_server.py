"""
Fake NWDAF(MTLF) Server for ML Model Provisioning Testing

Implements simplified Nnwdaf_MLModelProvision service per TS 29.520 §5.4
for E2E testing of NWDAF(AnLF) ML model integration.

Usage:
    uv run fake_mtlf_server.py [port]
    
Example:
    uv run fake_mtlf_server.py 8082
"""

import sys
import uuid
import asyncio
import logging
from datetime import datetime
from typing import Optional
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("fake_mtlf")

# ============================================================================
# Models (simplified from TS29520_Nnwdaf_MLModelProvision.yaml)
# ============================================================================

class TargetUeInformation(BaseModel):
    supis: Optional[list[str]] = None
    intGroupIds: Optional[list[str]] = Field(None, alias="intGroupIds")

class EventFilter(BaseModel):
    # Simplified - just placeholder
    pass

class MLEventSubscription(BaseModel):
    mLEvent: str  # NwdafEvent enum
    mLEventFilter: Optional[EventFilter] = None
    tgtUe: Optional[TargetUeInformation] = None

class ReportingInformation(BaseModel):
    immRep: Optional[bool] = None
    notifMethod: Optional[str] = None
    repPeriod: Optional[int] = None

class NwdafMLModelProvSubsc(BaseModel):
    """Request body for ML Model Provision subscription"""
    mLEventSubscs: list[MLEventSubscription]
    notifUri: str
    notifCorreId: Optional[str] = None
    eventReq: Optional[ReportingInformation] = None

class MLModelAddr(BaseModel):
    """ML Model file address"""
    mLModelUrl: Optional[str] = None
    mlFileFqdn: Optional[str] = None

class MLEventNotif(BaseModel):
    """Notification for a single ML event"""
    event: str
    notifCorreId: Optional[str] = None
    mLFileAddr: Optional[MLModelAddr] = None

class NwdafMLModelProvNotif(BaseModel):
    """Notification payload"""
    subscriptionId: str
    eventNotifs: list[MLEventNotif]

# ============================================================================
# Storage
# ============================================================================

subscriptions: dict[str, dict] = {}

# ============================================================================
# Configuration
# ============================================================================

# Default model URL - points to a mock model file
# In real scenario, this would be a trained model file URL
DEFAULT_MODEL_URL = "http://127.0.0.1:9090/models/ue_comm_default.h5"

# Delay before sending notification (simulates MTLF processing)
NOTIFICATION_DELAY_SECONDS = 1.0

# ============================================================================
# Background Tasks
# ============================================================================

async def send_notification(sub_id: str, notif_uri: str, events: list[MLEventSubscription], notif_corre_id: Optional[str]):
    """Send ML Model Provision notification to subscriber"""
    await asyncio.sleep(NOTIFICATION_DELAY_SECONDS)
    
    event_notifs = []
    for event_sub in events:
        event_notif = MLEventNotif(
            event=event_sub.mLEvent,
            notifCorreId=notif_corre_id,
            mLFileAddr=MLModelAddr(mLModelUrl=DEFAULT_MODEL_URL)
        )
        event_notifs.append(event_notif)
    
    notification = NwdafMLModelProvNotif(
        subscriptionId=sub_id,
        eventNotifs=event_notifs
    )
    
    # Per TS 29.520 §5.4.5.2: notification body is array of NwdafMLModelProvNotif
    payload = [notification.model_dump(exclude_none=True)]
    
    logger.info(f"Sending notification to {notif_uri}")
    logger.debug(f"Notification payload: {payload}")
    
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(
                notif_uri,
                json=payload,
                headers={"Content-Type": "application/json"}
            )
            if response.status_code == 204:
                logger.info(f"Notification accepted by {notif_uri}")
            else:
                logger.warning(f"Notification response: {response.status_code} from {notif_uri}")
    except Exception as e:
        logger.error(f"Failed to send notification to {notif_uri}: {e}")

# ============================================================================
# Application
# ============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Fake MTLF Server starting...")
    yield
    logger.info("Fake MTLF Server shutting down...")

app = FastAPI(
    title="Fake NWDAF(MTLF) Server",
    description="Mock ML Model Provisioning Service for E2E Testing",
    version="1.0.0",
    lifespan=lifespan
)

# ============================================================================
# Endpoints
# ============================================================================

@app.post("/nnwdaf-mlmodelprovision/v1/subscriptions", status_code=201)
async def create_subscription(
    request: NwdafMLModelProvSubsc,
    background_tasks: BackgroundTasks
):
    """
    Create ML Model Provision Subscription
    
    Per TS 29.520 §5.4.3.2.3.1:
    - Creates subscription resource
    - Returns 201 with subscription ID
    - Schedules notification with ML model URL
    """
    sub_id = str(uuid.uuid4())
    
    logger.info(f"Creating subscription {sub_id}")
    logger.info(f"  notifUri: {request.notifUri}")
    logger.info(f"  events: {[e.mLEvent for e in request.mLEventSubscs]}")
    
    # Store subscription
    subscriptions[sub_id] = {
        "request": request.model_dump(),
        "created_at": datetime.now().isoformat(),
        "status": "active"
    }
    
    # Schedule async notification
    background_tasks.add_task(
        send_notification,
        sub_id,
        request.notifUri,
        request.mLEventSubscs,
        request.notifCorreId
    )
    
    # Build response (echo back with subscription info)
    response_data = request.model_dump(exclude_none=True)
    
    return JSONResponse(
        status_code=201,
        content=response_data,
        headers={
            "Location": f"/nnwdaf-mlmodelprovision/v1/subscriptions/{sub_id}"
        }
    )

@app.delete("/nnwdaf-mlmodelprovision/v1/subscriptions/{subscription_id}", status_code=204)
async def delete_subscription(subscription_id: str):
    """
    Delete ML Model Provision Subscription
    
    Per TS 29.520 §5.4.3.3.3.2:
    - Removes subscription
    - Returns 204 No Content
    """
    if subscription_id not in subscriptions:
        raise HTTPException(status_code=404, detail="Subscription not found")
    
    del subscriptions[subscription_id]
    logger.info(f"Deleted subscription {subscription_id}")
    
    return None

@app.get("/subscriptions")
async def list_subscriptions():
    """Debug endpoint: list all active subscriptions"""
    return {"subscriptions": list(subscriptions.keys()), "count": len(subscriptions)}

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "fake-mtlf"}

# ============================================================================
# Main
# ============================================================================

def main():
    import uvicorn
    
    port = 8082
    if len(sys.argv) > 1:
        try:
            port = int(sys.argv[1])
        except ValueError:
            logger.error(f"Invalid port: {sys.argv[1]}")
            sys.exit(1)
    
    logger.info(f"Starting Fake MTLF Server on port {port}")
    logger.info(f"Default model URL: {DEFAULT_MODEL_URL}")
    uvicorn.run(app, host="127.0.0.1", port=port)

if __name__ == "__main__":
    main()
