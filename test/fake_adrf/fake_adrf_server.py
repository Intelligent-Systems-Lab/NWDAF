"""
Fake ADRF (Analytics Data Repository Function) Server

Implements simplified Nadrf_DataManagement service per TS 29.575
for E2E testing of the NWDAF ↔ ADRF integration.

Supported operations:
  StorageRequest        POST /nadrf-datamanagement/v1/data-store-records
  RetrievalRequest      GET  /nadrf-datamanagement/v1/data-store-records
  RetrievalSubscribe    POST /nadrf-datamanagement/v1/data-retrieval-subscriptions
  RetrievalUnsubscribe  DELETE /nadrf-datamanagement/v1/data-retrieval-subscriptions/{subscriptionId}

Flow:
  1. NWDAF stores UPF records via StorageRequest → ADRF returns storeTransId per record.
  2. NWDAF subscribes for retrieval (consTrigNotif=true) → ADRF sends fetchInstruct
     callback(s) containing batches of storeTransIds (= fetchCorrIds).
  3. NWDAF GETs actual data via RetrievalRequest using the received fetchCorrIds.
  4. NWDAF cleans up via RetrievalUnsubscribe.

Usage:
    uv run fake_adrf_server.py [--port PORT] [--delay SECONDS] [--batch-size N]

Examples:
    uv run fake_adrf_server.py
    uv run fake_adrf_server.py --port 9888 --delay 2 --batch-size 10
"""

import sys
import uuid
import asyncio
import logging
import argparse
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from typing import Optional

import httpx
from fastapi import FastAPI, Request, HTTPException, Response
from fastapi.responses import JSONResponse

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("fake_adrf")

# ============================================================================
# CLI config
# ============================================================================

cfg: dict = {
    "port": 9888,
    "delay": 1,
    "batch_size": 30,
}

# ============================================================================
# In-memory storage
# ============================================================================

# storeTransId → NadrfDataStoreRecord (raw dict)
records: dict[str, dict] = {}

# Preserve insertion order so filtering is deterministic
record_order: list[str] = []

# subscriptionId → subscription info dict
subscriptions: dict[str, dict] = {}

# ============================================================================
# Helpers
# ============================================================================

BASE_PATH = "/nadrf-datamanagement/v1"


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def get_supi_from_record(record: dict) -> Optional[str]:
    """Extract supi from dataSub[0].smfDataSub.supi if present."""
    data_sub_list = record.get("dataSub", [])
    if not data_sub_list:
        return None
    first = data_sub_list[0]
    smf = first.get("smfDataSub", {})
    return smf.get("supi")


def filter_records(supi_filter: Optional[str]) -> list[str]:
    """Return storeTransIds matching the supi filter (or all if no filter)."""
    result = []
    for tid in record_order:
        rec = records.get(tid)
        if rec is None:
            continue
        if supi_filter is None:
            result.append(tid)
        else:
            if get_supi_from_record(rec) == supi_filter:
                result.append(tid)
    return result


def merge_records(trans_ids: list[str]) -> Optional[dict]:
    """
    Merge matching records into a single NadrfDataStoreRecord.
    dataSub is taken from the first record; upfEventNotifs are concatenated.
    Returns None if no records match.
    """
    matched = [records[tid] for tid in trans_ids if tid in records]
    if not matched:
        return None

    merged = {
        "dataSub": matched[0].get("dataSub", []),
        "dataNotif": {},
    }

    all_upf_notifs = []
    for rec in matched:
        notif = rec.get("dataNotif", {})
        all_upf_notifs.extend(notif.get("upfEventNotifs", []))

    if all_upf_notifs:
        merged["dataNotif"]["upfEventNotifs"] = all_upf_notifs
    else:
        merged.pop("dataNotif")

    return merged


# ============================================================================
# Async callback: send fetchInstruct notification(s) to NWDAF
# ============================================================================

async def send_retrieval_notifications(
    sub_id: str,
    notif_corr_id: str,
    notification_uri: str,
    matching_ids: list[str],
    fetch_uri: str,
) -> None:
    await asyncio.sleep(cfg["delay"])

    batch_size = cfg["batch_size"]
    batches = [
        matching_ids[i : i + batch_size]
        for i in range(0, max(len(matching_ids), 1), batch_size)
    ] if matching_ids else [[]]

    for idx, batch in enumerate(batches):
        is_last = idx == len(batches) - 1

        notification = {
            "notifCorrId": notif_corr_id,
            "timeStamp": now_iso(),
            "fetchInstruct": {
                "fetchUri": fetch_uri,
                "fetchCorrIds": batch,
            },
        }
        if is_last:
            notification["terminationReq"] = True

        logger.info(
            f"[sub={sub_id}] Sending callback {idx+1}/{len(batches)} to {notification_uri} "
            f"with {len(batch)} ID(s), terminationReq={is_last}"
        )
        logger.debug(f"Notification: {notification}")

        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.post(
                    notification_uri,
                    json=notification,
                    headers={"Content-Type": "application/json"},
                )
                if resp.status_code == 204:
                    logger.info(f"[sub={sub_id}] Callback {idx+1} acknowledged (204)")
                else:
                    logger.warning(
                        f"[sub={sub_id}] Callback {idx+1} response: {resp.status_code}"
                    )
        except Exception as e:
            logger.error(f"[sub={sub_id}] Callback {idx+1} failed: {e}")


# ============================================================================
# App
# ============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(
        f"Fake ADRF server ready  port={cfg['port']}  "
        f"delay={cfg['delay']}s  batch_size={cfg['batch_size']}"
    )
    yield


app = FastAPI(
    title="Fake ADRF Server",
    description="Mock Nadrf_DataManagement Service (TS 29.575) for E2E Testing",
    version="0.1.0",
    lifespan=lifespan,
)

# ============================================================================
# StorageRequest: POST /nadrf-datamanagement/v1/data-store-records
# ============================================================================

@app.post(f"{BASE_PATH}/data-store-records", status_code=201)
async def create_data_store_record(request: Request):
    """
    StorageRequest — store a NadrfDataStoreRecord.
    Returns 201 with Location header containing the storeTransId.
    """
    body = await request.json()
    store_trans_id = str(uuid.uuid4())

    records[store_trans_id] = body
    record_order.append(store_trans_id)

    supi = get_supi_from_record(body)
    logger.info(f"Stored record  storeTransId={store_trans_id}  supi={supi}")

    location = f"{BASE_PATH}/data-store-records/{store_trans_id}"
    return JSONResponse(
        status_code=201,
        content=body,
        headers={"Location": location},
    )


# ============================================================================
# RetrievalRequest: GET /nadrf-datamanagement/v1/data-store-records
# ============================================================================

@app.get(f"{BASE_PATH}/data-store-records")
async def get_data_store_records(request: Request):
    """
    RetrievalRequest — fetch records by fetch-correlation-ids (comma-separated).
    Returns 200 with merged NadrfDataStoreRecord, or 204 if no records found.
    """
    raw = request.query_params.get("fetch-correlation-ids", "")
    if not raw:
        raise HTTPException(status_code=400, detail="fetch-correlation-ids is required")

    ids = [s.strip() for s in raw.split(",") if s.strip()]
    logger.info(f"RetrievalRequest  ids={ids}")

    merged = merge_records(ids)
    if merged is None:
        logger.info("No matching records found, returning 204")
        return Response(status_code=204)

    notif_count = len(
        merged.get("dataNotif", {}).get("upfEventNotifs", [])
    )
    logger.info(f"Returning merged record with {notif_count} upfEventNotif(s)")
    return JSONResponse(status_code=200, content=merged)


# ============================================================================
# RetrievalSubscribe: POST /nadrf-datamanagement/v1/data-retrieval-subscriptions
# ============================================================================

@app.post(f"{BASE_PATH}/data-retrieval-subscriptions", status_code=201)
async def create_retrieval_subscription(request: Request):
    """
    RetrievalSubscribe — create a data retrieval subscription.
    When consTrigNotif=true, sends fetchInstruct callback(s) instead of raw data.
    Returns 201 with Location header and the subscription resource.
    """
    body = await request.json()
    sub_id = str(uuid.uuid4())

    notif_corr_id = body.get("notifCorrId", sub_id)
    notification_uri = body.get("notificationURI", "")
    cons_trig_notif = body.get("consTrigNotif", False)

    # Extract supi filter from dataSub.smfDataSub.supi
    data_sub = body.get("dataSub", {})
    smf_data_sub = data_sub.get("smfDataSub", {})
    supi_filter = smf_data_sub.get("supi")

    subscriptions[sub_id] = {
        "body": body,
        "notifCorrId": notif_corr_id,
        "notificationURI": notification_uri,
        "consTrigNotif": cons_trig_notif,
        "supiFilter": supi_filter,
    }

    logger.info(
        f"Retrieval subscription created  sub_id={sub_id}  "
        f"notifCorrId={notif_corr_id}  supiFilter={supi_filter}  "
        f"consTrigNotif={cons_trig_notif}  notificationURI={notification_uri}"
    )

    # Build the fetchUri (ADRF's own data-store-records endpoint)
    host = request.headers.get("host", f"127.0.0.1:{cfg['port']}")
    fetch_uri = f"http://{host}{BASE_PATH}/data-store-records"

    # Find matching records now (snapshot at subscription time)
    matching_ids = filter_records(supi_filter)
    logger.info(
        f"[sub={sub_id}] Found {len(matching_ids)} matching record(s) for supi={supi_filter}"
    )

    if notification_uri:
        asyncio.create_task(
            send_retrieval_notifications(
                sub_id, notif_corr_id, notification_uri, matching_ids, fetch_uri
            )
        )
    else:
        logger.warning(f"[sub={sub_id}] No notificationURI; callback skipped")

    location = f"{BASE_PATH}/data-retrieval-subscriptions/{sub_id}"
    return JSONResponse(
        status_code=201,
        content=body,
        headers={"Location": location},
    )


# ============================================================================
# RetrievalUnsubscribe: DELETE /nadrf-datamanagement/v1/data-retrieval-subscriptions/{id}
# ============================================================================

@app.delete(
    f"{BASE_PATH}/data-retrieval-subscriptions/{{subscription_id}}",
    status_code=204,
)
async def delete_retrieval_subscription(subscription_id: str):
    """
    RetrievalUnsubscribe — delete an Individual ADRF Data Retrieval Subscription.
    Returns 204 No Content.
    """
    if subscription_id not in subscriptions:
        raise HTTPException(status_code=404, detail="Subscription not found")

    del subscriptions[subscription_id]
    logger.info(f"Deleted subscription  sub_id={subscription_id}")
    return Response(status_code=204)


# ============================================================================
# Debug / health endpoints
# ============================================================================

@app.get("/health")
async def health():
    return {"status": "healthy", "service": "fake-adrf"}


@app.get("/debug/records")
async def debug_records():
    return {
        "count": len(records),
        "storeTransIds": record_order,
    }


@app.get("/debug/subscriptions")
async def debug_subscriptions():
    return {
        "count": len(subscriptions),
        "subscriptions": {
            sid: {
                "notifCorrId": info["notifCorrId"],
                "supiFilter": info["supiFilter"],
                "notificationURI": info["notificationURI"],
            }
            for sid, info in subscriptions.items()
        },
    }


# ============================================================================
# Main
# ============================================================================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fake ADRF Server (TS 29.575)")
    parser.add_argument("--port", type=int, default=9888,
                        help="Port to listen on (default: 9888)")
    parser.add_argument("--delay", type=float, default=1.0,
                        help="Seconds before sending retrieval callback (default: 1)")
    parser.add_argument("--batch-size", type=int, default=30,
                        help="Max fetchCorrIds per callback notification (default: 30)")
    return parser.parse_args()


def main():
    args = parse_args()
    cfg["port"] = args.port
    cfg["delay"] = args.delay
    cfg["batch_size"] = args.batch_size

    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=args.port)


if __name__ == "__main__":
    main()
