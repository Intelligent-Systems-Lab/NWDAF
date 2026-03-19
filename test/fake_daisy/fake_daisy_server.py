"""
Fake Daisy FL Server for NWDAF Async Callback Testing

Simulates the async training flow:
  1. NWDAF sends POST /publish_task with CALLBACK_URL and TID
  2. Daisy responds 202 Accepted immediately
  3. After a configurable delay, Daisy POSTs training result to CALLBACK_URL

Usage:
    uv run fake_daisy_server.py [--port PORT] [--delay SECONDS] [--model-url URL] [--fail]

Examples:
    uv run fake_daisy_server.py
    uv run fake_daisy_server.py --port 9887 --delay 5
    uv run fake_daisy_server.py --delay 3 --model-url nwdaf_ml_service/ml/artifacts/model_new.npy
    uv run fake_daisy_server.py --fail   # simulate training failure
"""

import sys
import asyncio
import logging
import argparse
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("fake_daisy")

# ============================================================================
# CLI args (parsed at startup, stored as module-level config)
# ============================================================================

cfg: dict = {
    "delay": 3,
    "model_url": "nwdaf_ml_service/ml/artifacts/model_new.npy",
    "fail": False,
}


# ============================================================================
# Background task: call back NWDAF after training "completes"
# ============================================================================

async def send_callback(task_id: str, callback_url: str) -> None:
    await asyncio.sleep(cfg["delay"])

    if cfg["fail"]:
        payload = {
            "task_id": task_id,
            "status": "failure",
            "error": "simulated training failure",
        }
    else:
        payload = {
            "task_id": task_id,
            "model_url": cfg["model_url"],
            "status": "success",
        }

    logger.info(f"Sending callback to {callback_url}: {payload}")
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(callback_url, json=payload)
            logger.info(f"Callback response: {resp.status_code}")
    except Exception as e:
        logger.error(f"Callback failed: {e}")


# ============================================================================
# App
# ============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(
        f"Fake Daisy server ready  delay={cfg['delay']}s  "
        f"model_url={cfg['model_url']}  fail={cfg['fail']}"
    )
    yield


app = FastAPI(title="Fake Daisy FL Server", version="0.1.0", lifespan=lifespan)


@app.post("/publish_task", status_code=202)
async def publish_task(request: Request):
    """
    Accept a training task and respond 202 immediately.
    Schedules a background callback to NWDAF after cfg['delay'] seconds.
    """
    body = await request.json()
    task_id = body.get("TID", "unknown")
    callback_url = body.get("CALLBACK_URL", "")

    logger.info(f"Received task: TID={task_id} CALLBACK_URL={callback_url}")
    logger.debug(f"Full payload: {body}")

    if not callback_url:
        logger.warning("No CALLBACK_URL in task; will not send callback")
        return JSONResponse(status_code=202, content={"accepted": True, "task_id": task_id})

    asyncio.create_task(send_callback(task_id, callback_url))
    return JSONResponse(status_code=202, content={"accepted": True, "task_id": task_id})


@app.get("/health")
async def health():
    return {"status": "healthy", "service": "fake-daisy"}


# ============================================================================
# Main
# ============================================================================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fake Daisy FL Server")
    parser.add_argument("--port", type=int, default=9887)
    parser.add_argument("--delay", type=int, default=3,
                        help="Seconds to wait before sending callback (default: 3)")
    parser.add_argument("--model-url", default="nwdaf_ml_service/ml/artifacts/model_new.npy",
                        help="model_url sent back in the success callback")
    parser.add_argument("--fail", action="store_true",
                        help="Simulate training failure in the callback")
    return parser.parse_args()


def main():
    args = parse_args()
    cfg["delay"] = args.delay
    cfg["model_url"] = args.model_url
    cfg["fail"] = args.fail

    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=args.port)


if __name__ == "__main__":
    main()
