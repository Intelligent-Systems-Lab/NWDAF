# NWDAF Testing Guide

---

## 1. Environment Setup

### 1.1 Build Project
```bash
make build
```

### 1.2 Python Test Environment (Notification Callback)
```bash
cd test/callback
uv sync  # Uses uv to manage Python environment
```

### 1.3 Required Tools
- Go 1.21+
- jq (JSON parsing)
- curl
- Python 3.10+ (managed by uv)

---

## 2. Unit Tests

### 2.1 Run All Unit Tests
```bash
# Run all unit tests
go test ./internal/... -v

# Run processor tests only
go test ./internal/sbi/processor/... -v

# Run notifier tests only
go test ./internal/notifier/... -v

# Run collector tests only
go test ./internal/collector/... -v
```

### 2.2 Processor Tests

Location: `internal/sbi/processor/eventssubscription_test.go`

| Test Function | Description |
|---------------|-------------|
| `TestValidateSupportedEvent` | Event type validation |
| `TestValidateUeCommunication` | UE_COMMUNICATION validation (tgtUe, supis/intGroupIds) |
| `TestValidateEvtReq` | evtReq validation (PERIODIC/repPeriod/maxReportNbr) |
| `TestCollectFailEventReports` | failEventReports collection |
| `TestValidateEventTargetPeriod` | startTs/endTs validation |

### 2.3 Notifier Tests

Location: `internal/notifier/notifier_test.go`

| Test Function | Description |
|---------------|-------------|
| `TestShouldContinue_MaxReportNbrLimit` | Verify scheduler stops at maxReportNbr |
| `TestShouldContinue_MonDurExpiry` | Verify scheduler stops when monDur expires |
| `TestBuildNotification_NotifCorrId` | Verify notifCorrId in notification |

### 2.4 Collector Tests

Location: `internal/collector/collector_test.go`

| Test Function | Description |
|---------------|-------------|
| `TestCollectorContext_StoreAndRetrieveUeData` | UE data CRUD |
| `TestHandleNotification` | SMF notification handling |
| `TestHandleUpfNotification` | UPF notification (volume + throughput) |

---

## 3. API Integration Tests

### 3.1 Run Command
```bash
# Start NWDAF server first
./bin/nwdaf --config config/nwdafcfg.yaml

# Run tests in another terminal
./test/scripts/test_api.sh all
```

### 3.2 Test Commands

| Command | Test Description | Expected |
|---------|------------------|----------|
| `create` | Create valid UE_COMMUNICATION subscription | 201 |
| `intgroup` | UE_COMMUNICATION with intGroupIds | 201 |
| `missing-tgtue` | Missing tgtUe | 400 |
| `missing-id` | Empty tgtUe (no supis/intGroupIds) | 400 |
| `unsupported` | Unsupported event type (UE_MOBILITY) | 400 |
| `abnormal` | ABNORMAL_BEHAVIOUR → failEventReports | 201 |
| `evtreq` | PERIODIC without repPeriod | 400 |
| `evtreq-valid` | Valid PERIODIC + repPeriod | 201 |
| `target_period` | startTs in past + endTs in future | 400 |
| `delete <id>` | Delete subscription | 204 |

---

## 4. Notification Mechanism Testing

### 4.1 Start Callback Server
```bash
cd test/callback
uv run callback_server.py 9090
```

Expected output:
```
🚀 Callback server listening on http://localhost:9090
Supported events: UE_COMMUNICATION
Press Ctrl+C to stop
```

### 4.2 Create Periodic Subscription
```bash
curl -X POST http://localhost:8080/nnwdaf-eventssubscription/v1/subscriptions \
  -H "Content-Type: application/json" \
  -d '{
    "eventSubscriptions": [{
      "event": "UE_COMMUNICATION",
      "tgtUe": {"supis": ["imsi-208930000000003"]}
    }],
    "notificationURI": "http://localhost:9090/callback",
    "evtReq": {"notifMethod": "PERIODIC", "repPeriod": 10}
  }'
```

### 4.3 Expected Notification Output
```
============================================================
[15:30:00] 📩 Notification Received
============================================================
Subscription ID: abc123-def456

--- Event 1 ---
Event Type: UE_COMMUNICATION

  📱 UE Communication 1:
    Communication Duration: 300s
    Timestamp: 2026-01-14T12:00:00Z
    Traffic Info:
      DNN: internet
      Uplink Volume: 1.0 MB
      Downlink Volume: 4.9 MB
    Confidence: 90%
============================================================
```

### 4.4 Test maxReportNbr Limit

Verify scheduler stops after max reports:

```bash
curl -X POST http://localhost:8080/nnwdaf-eventssubscription/v1/subscriptions \
  -H "Content-Type: application/json" \
  -d '{
    "eventSubscriptions": [{
      "event": "UE_COMMUNICATION",
      "tgtUe": {"supis": ["imsi-208930000000003"]}
    }],
    "notificationURI": "http://localhost:9090/callback",
    "evtReq": {
      "notifMethod": "PERIODIC",
      "repPeriod": 2,
      "maxReportNbr": 3
    }
  }'
```

**Expected**:
- Callback server receives exactly 3 notifications
- NWDAF logs: `notification completed: LIMIT_REACHED`

### 4.5 Test monDur Expiry

```bash
# Generate future time (30 seconds from now)
MONDUR=$(date -u -d "+30 seconds" +"%Y-%m-%dT%H:%M:%SZ")

curl -X POST http://localhost:8080/nnwdaf-eventssubscription/v1/subscriptions \
  -H "Content-Type: application/json" \
  -d '{
    "eventSubscriptions": [{
      "event": "UE_COMMUNICATION",
      "tgtUe": {"supis": ["imsi-208930000000003"]}
    }],
    "notificationURI": "http://localhost:9090/callback",
    "evtReq": {
      "notifMethod": "PERIODIC",
      "repPeriod": 5,
      "monDur": "'"$MONDUR"'"
    }
  }'
```

**Expected**: Notifications stop after ~30 seconds.

### 4.6 Test notifCorrId Passthrough

```bash
curl -X POST http://localhost:8080/nnwdaf-eventssubscription/v1/subscriptions \
  -H "Content-Type: application/json" \
  -d '{
    "eventSubscriptions": [{
      "event": "UE_COMMUNICATION",
      "tgtUe": {"supis": ["imsi-208930000000003"]}
    }],
    "notificationURI": "http://localhost:9090/callback",
    "notifCorrId": "my-correlation-id-12345",
    "evtReq": {"notifMethod": "PERIODIC", "repPeriod": 10}
  }'
```

**Expected**: Notification contains `Correlation ID: my-correlation-id-12345`

---

## 5. Test Directory Structure

```
test/
├── callback/                # Python notification callback testing
│   ├── callback_server.py   # HTTP callback server
│   ├── pyproject.toml       # uv project configuration
│   └── README.md            # Usage instructions
└── scripts/
    └── test_api.sh          # API integration test script
```

---

## 6. Complete Test Workflow

### Step 1: Build & Run Unit Tests
```bash
cd /path/to/NWDAF
make build
go test ./internal/... -v
```

### Step 2: API Integration Tests

**Terminal 1** - Start NWDAF:
```bash
./bin/nwdaf --config config/nwdafcfg.yaml
```

**Terminal 2** - Run API tests:
```bash
./test/scripts/test_api.sh all
```

### Step 3: Notification Tests

**Terminal 1** - Start callback server:
```bash
cd test/callback && uv run callback_server.py 9090
```

**Terminal 2** - Start NWDAF (if not running):
```bash
./bin/nwdaf --config config/nwdafcfg.yaml
```

**Terminal 3** - Create PERIODIC subscription:
```bash
curl -X POST http://localhost:8080/nnwdaf-eventssubscription/v1/subscriptions \
  -H "Content-Type: application/json" \
  -d '{
    "eventSubscriptions": [{
      "event": "UE_COMMUNICATION",
      "tgtUe": {"supis": ["imsi-208930000000003"]}
    }],
    "notificationURI": "http://localhost:9090/callback",
    "evtReq": {"notifMethod": "PERIODIC", "repPeriod": 10}
  }'
```

---

## 7. Error Code Reference

| HTTP | Cause | Description |
|------|-------|-------------|
| 400 | `INVALID_REQUEST` | Request format error |
| 400 | `ALL_EVENTS_UNSUPPORTED` | All events unsupported |
| 400 | `BOTH_STAT_PRED_NOT_ALLOWED` | Statistics + prediction not allowed |
| 404 | `SUBSCRIPTION_NOT_FOUND` | Subscription does not exist |
