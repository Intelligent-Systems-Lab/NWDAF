# NWDAF Testing Guide

**Last Updated**: 2026-01-07

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

### 2.1 Run Command
```bash
go test ./internal/sbi/processor/... -v
```

### 2.2 Test Functions (11 tests)

| Test Function | Description | Cases |
|---------------|-------------|-------|
| `TestValidateSupportedEvent` | Event type validation | 3 |
| `TestValidateAbnormalBehaviour` | ABNORMAL_BEHAVIOUR validation | 8 |
| `TestIsMobilityRelated` | Mobility-related exception check | 4 |
| `TestIsCommunRelated` | Communication-related exception check | 4 |
| `TestValidateEvtReq` | evtReq validation (PERIODIC/repPeriod) | 5 |
| `TestValidateSupportedExceptionIds` | ExceptionId support check | 4 |
| `TestValidateExptAnaType` | exptAnaType support check | 4 |
| `TestCheckUnsupportedExceptionIds` | ExceptionId validation → 400 | 3 |
| `TestCollectFailEventReports` | failEventReports collection | 3 |
| `TestValidateEventTargetPeriod` | startTs/endTs validation | 5 |

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
| `create` | Create valid subscription | 201 |
| `mutual` | excepRequs/exptAnaType mutual exclusion | 400 |
| `anyue` | anyUe missing required fields | 400 |
| `unsupported` | Unsupported event type | 400 |
| `evtreq` | PERIODIC without repPeriod | 400 |
| `exception` | Mixed events (failEventReports) | 201 |
| `anatype` | Unsupported exptAnaType | 400 |
| `evtreq-valid` | Valid evtReq | 201 |
| `target_period` | startTs in past + endTs in future | 400 |
| `delete <id>` | Delete subscription | 204 |

### 3.3 Run Individual Tests
```bash
./test/scripts/test_api.sh create      # Single test
./test/scripts/test_api.sh all         # All tests
./test/scripts/test_api.sh delete abc  # Delete specific ID
```

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
Press Ctrl+C to stop
```

### 4.2 Create Periodic Subscription
```bash
curl -X POST http://localhost:8080/nnwdaf-eventssubscription/v1/subscriptions \
  -H "Content-Type: application/json" \
  -d '{
    "eventSubscriptions": [{
      "event": "ABNORMAL_BEHAVIOUR",
      "tgtUe": {"anyUe": true},
      "excepRequs": [{"excepId": "SUSPICION_OF_DDOS_ATTACK"}],
      "dnns": ["internet"]
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
Event Type: ABNORMAL_BEHAVIOUR

  Abnormal Behaviour 1:
    ExceptionId: SUSPICION_OF_DDOS_ATTACK
    ExceptionLevel: 3
    ExceptionTrend: UP
    Ratio: 15%
    Confidence: 85%
    DDoS Attack IPs: ['192.168.1.100', '192.168.1.101']
============================================================
```

### 4.4 Test Subscription Update
```bash
# Update period to 5 seconds
curl -X PUT http://localhost:8080/nnwdaf-eventssubscription/v1/subscriptions/{id} \
  -H "Content-Type: application/json" \
  -d '{...evtReq.repPeriod: 5...}'
```

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

### Step 1: Unit Tests
```bash
make build
go test ./internal/sbi/processor/... -v
# Expected: 11 tests PASS
```

### Step 2: API Tests
```bash
# Terminal 1
./bin/nwdaf --config config/nwdafcfg.yaml

# Terminal 2
./test/scripts/test_api.sh all
# Expected: 9 tests PASS
```

### Step 3: Notification Tests
```bash
# Terminal 1: Callback server
cd test/callback && uv run callback_server.py 9090

# Terminal 2: NWDAF server
./bin/nwdaf --config config/nwdafcfg.yaml

# Terminal 3: Create PERIODIC subscription
curl -X POST ... (see section 4.2)
# Expected: Receive notifications every 10 seconds
```

---

## 7. Error Code Reference

| HTTP | Cause | Description |
|------|-------|-------------|
| 400 | `INVALID_REQUEST` | Request format error |
| 400 | `ALL_EVENTS_UNSUPPORTED` | All events unsupported |
| 400 | `UNSUPPORTED_EXCEPTION_ID` | ExceptionId not supported |
| 400 | `UNSUPPORTED_ANALYTICS_TYPE` | exptAnaType not supported |
| 400 | `BOTH_STAT_PRED_NOT_ALLOWED` | Statistics + prediction not allowed |
| 404 | `SUBSCRIPTION_NOT_FOUND` | Subscription does not exist |
