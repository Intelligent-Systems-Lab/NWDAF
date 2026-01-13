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
```

### 2.2 Processor Tests (20 tests)

Location: `internal/sbi/processor/eventssubscription_test.go`

| Test Function | Description | Cases |
|---------------|-------------|-------|
| `TestValidateSupportedEvent` | Event type validation | 4 |
| `TestValidateUeCommunication` | UE_COMMUNICATION validation (tgtUe, supis/intGroupIds) | 5 |
| `TestValidateAbnormalBehaviour` | ABNORMAL_BEHAVIOUR validation (tgtUe, excepRequs/exptAnaType) | 5 |
| `TestIsMobilityRelated` | Mobility-related exception check | 3 |
| `TestIsCommunRelated` | Communication-related exception check | 3 |
| `TestValidateEvtReq` | evtReq validation (PERIODIC/repPeriod/maxReportNbr) | 5 |
| `TestValidateSupportedExceptionIds` | ExceptionId support check | 4 |
| `TestValidateExptAnaType` | exptAnaType support check | 4 |
| `TestCheckUnsupportedExceptionIds` | ExceptionId validation → 400 | 3 |
| `TestCollectFailEventReports` | failEventReports collection | 3 |
| `TestValidateEventTargetPeriod` | startTs/endTs validation (BOTH_STAT_PRED_NOT_ALLOWED) | 5 |

### 2.3 Notifier Tests (7 tests)

Location: `internal/notifier/notifier_test.go`

| Test Function | Description | Cases |
|---------------|-------------|-------|
| `TestShouldContinue_MaxReportNbrLimit` | Verify scheduler stops at maxReportNbr | 4 |
| `TestShouldContinue_MonDurExpiry` | Verify scheduler stops when monDur expires | 3 |
| `TestShouldContinue_CombinedLimits` | Test combined maxReportNbr + monDur | 4 |
| `TestBuildNotification_NotifCorrId` | Verify notifCorrId in notification | 2 |
| `TestBuildNotification_SubscriptionId` | Verify subscriptionId always included | 1 |
| `TestHandleCompletion_Callback` | Verify onComplete callback invocation | 1 |
| `TestNewNotificationScheduler` | Verify scheduler initialization | 1 |

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
| `uecomm` | UE_COMMUNICATION valid subscription | 201 |
| `uecomm-invalid` | UE_COMMUNICATION missing tgtUe | 400 |
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
# Update period to 5 seconds (replace {id} with actual subscription ID)
curl -X PUT http://localhost:8080/nnwdaf-eventssubscription/v1/subscriptions/{id} \
  -H "Content-Type: application/json" \
  -d '{
    "eventSubscriptions": [{
      "event": "ABNORMAL_BEHAVIOUR",
      "tgtUe": {"anyUe": true},
      "excepRequs": [{"excepId": "SUSPICION_OF_DDOS_ATTACK"}],
      "dnns": ["internet"]
    }],
    "notificationURI": "http://localhost:9090/callback",
    "evtReq": {"notifMethod": "PERIODIC", "repPeriod": 5}
  }'
```

**Expected**: 
- Response 200 OK with updated subscription
- Notifications now sent every 5 seconds instead of original period

### 4.5 Test maxReportNbr Limit

Verify scheduler stops after max reports:

```bash
# Create subscription with maxReportNbr=3, repPeriod=2
curl -X POST http://localhost:8080/nnwdaf-eventssubscription/v1/subscriptions \
  -H "Content-Type: application/json" \
  -d '{
    "eventSubscriptions": [{
      "event": "ABNORMAL_BEHAVIOUR",
      "tgtUe": {"anyUe": true},
      "exptAnaType": "COMMUN",
      "appIds": ["app1"]
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
- NWDAF logs: `Subscription xxx notification completed: LIMIT_REACHED (sent 3 reports)`

### 4.6 Test monDur Expiry

Verify scheduler stops when monitoring duration expires:

**Step 1**: Generate a future time (30 seconds from now)
```bash
MONDUR=$(date -u -d "+30 seconds" +"%Y-%m-%dT%H:%M:%SZ")
echo "MonDur will be: $MONDUR"
```

**Step 2**: Create subscription (copy the JSON and replace the monDur value manually)
```bash
curl -X POST http://localhost:8080/nnwdaf-eventssubscription/v1/subscriptions \
  -H "Content-Type: application/json" \
  -d '{
    "eventSubscriptions": [{
      "event": "ABNORMAL_BEHAVIOUR",
      "tgtUe": {"anyUe": true},
      "exptAnaType": "COMMUN",
      "appIds": ["app1"]
    }],
    "notificationURI": "http://localhost:9090/callback",
    "evtReq": {
      "notifMethod": "PERIODIC",
      "repPeriod": 5,
      "monDur": "PASTE_YOUR_MONDUR_HERE"
    }
  }'
```

> **💡 Tip**: Replace `PASTE_YOUR_MONDUR_HERE` with the value from Step 1 (e.g., `2026-01-07T11:05:00Z`)

**Expected**:
- NWDAF logs show: `monDur: 2026-01-07T11:05:00Z` (your generated time)
- Notifications sent every 5 seconds until monDur passes
- After ~30 seconds, logs show: `notification completed: LIMIT_REACHED`

### 4.7 Test notifCorrId Passthrough

Verify notification contains notifCorrId:

```bash
curl -X POST http://localhost:8080/nnwdaf-eventssubscription/v1/subscriptions \
  -H "Content-Type: application/json" \
  -d '{
    "eventSubscriptions": [{
      "event": "ABNORMAL_BEHAVIOUR",
      "tgtUe": {"anyUe": true},
      "exptAnaType": "COMMUN",
      "appIds": ["app1"]
    }],
    "notificationURI": "http://localhost:9090/callback",
    "notifCorrId": "my-correlation-id-12345",
    "evtReq": {"notifMethod": "PERIODIC", "repPeriod": 10}
  }'
```

**Expected**:
- Notification JSON contains: `"notifCorrId": "my-correlation-id-12345"`
- Callback server logs show the correlation ID

### 4.8 Test UE_COMMUNICATION Subscription

Create UE Communication subscription for N4 Session Inactivity Timer:

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

**Expected Notification**:
```json
{
  "subscriptionId": "xxx",
  "eventNotifications": [{
    "event": "UE_COMMUNICATION",
    "ueComms": [{
      "commDur": 300,
      "ts": "2026-01-12T16:00:00Z",
      "trafChar": {"dnn": "internet", "ulVol": 1024000, "dlVol": 5120000}
    }]
  }]
}

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

## 6. Complete Test Workflow (Step-by-Step)

Follow these steps in order. You need **3 terminal windows**.

### Step 1: Build & Run Unit Tests
```bash
# In any terminal
cd /path/to/NWDAF
make build
go test ./internal/... -v
```
**Expected**: 24 tests PASS (17 processor + 7 notifier)

---

### Step 2: API Integration Tests

**Terminal 1** - Start NWDAF:
```bash
./bin/nwdaf --config config/nwdafcfg.yaml
```

**Terminal 2** - Run API tests:
```bash
./test/scripts/test_api.sh all
```
**Expected**: 9 tests PASS

---

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
      "event": "ABNORMAL_BEHAVIOUR",
      "tgtUe": {"anyUe": true},
      "excepRequs": [{"excepId": "SUSPICION_OF_DDOS_ATTACK"}],
      "dnns": ["internet"]
    }],
    "notificationURI": "http://localhost:9090/callback",
    "evtReq": {"notifMethod": "PERIODIC", "repPeriod": 10}
  }'
```
**Expected**: Terminal 1 shows notifications every 10 seconds

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
