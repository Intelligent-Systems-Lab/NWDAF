# Testing Guide

This document describes how to run tests for the NWDAF EventSubscription service.

## Prerequisites

- Go 1.21+
- `jq` (for JSON parsing in shell scripts)
- `curl`

## 1. Starting the Server

Before running API tests, you must start the NWDAF server.

```bash
# Build and run
make build
./bin/nwdaf --config config/nwdafcfg.yaml
```

The server listens on `http://127.0.0.1:8080`.

---

## 2. Go Unit Tests

Unit tests can be run without starting the server.

### Run All Tests

```bash
go test ./... -v
```

### Run Processor Tests

```bash
go test ./internal/sbi/processor/... -v
```

### Unit Test Coverage

| Test | Cases | Phase |
|------|-------|-------|
| `TestValidateSupportedEvent` | 3 | 2A |
| `TestValidateAbnormalBehaviour` | 5 | 2A |
| `TestIsMobilityRelated` | 3 | 2A |
| `TestIsCommunRelated` | 3 | 2A |
| `TestValidateEvtReq` | 5 | 2B |
| `TestValidateSupportedExceptionIds` | 4 | 2B |
| `TestValidateExptAnaType` | 4 | 2B |
| `TestCheckUnsupportedExceptionIds` | 3 | 2C |
| `TestCheckUnsupportedExptAnaType` | 3 | 2C |
| `TestCollectFailEventReports` | 4 | 2C |
| `TestValidateAnalyticsTargetPeriod` | 5 | **2C** |

**Total: 11 tests, 42 cases**

---

## 3. API Integration Tests

### Quick Start

```bash
# Terminal 1: Start server
./bin/nwdaf --config config/nwdafcfg.yaml

# Terminal 2: Run all tests
./test/scripts/test_api.sh all
```

### Test Commands

| Command | Description |
|---------|-------------|
| `all` | Run all tests |
| `create` | Valid subscription |
| `mutual` | excepRequs/exptAnaType mutual exclusion |
| `anyue` | anyUe missing fields |
| `unsupported` | Unsupported event type |
| `evtreq` | PERIODIC without repPeriod |
| `exception` | Mixed ExceptionIds (failEventReports) |
| `anatype` | ALL_EVENTS_UNSUPPORTED |
| `evtreq-valid` | Valid evtReq |
| `target_period` | startTs past + endTs future (BOTH_STAT_PRED_NOT_ALLOWED) |
| `delete <id>` | Delete subscription |

---

## 4. Phase 2C Test Cases

### 4.1 Mixed Events (failEventReports)

**Command:** `./test_api.sh exception`

**Description:** Creates a subscription with both supported and unsupported ExceptionIds.

**Request:**
```json
{
  "eventSubscriptions": [
    {"excepRequs": [{"excepId": "SUSPICION_OF_DDOS_ATTACK"}]},
    {"excepRequs": [{"excepId": "UNEXPECTED_UE_LOCATION"}]}
  ]
}
```

**Expected Result:** `201 Created` with `failEventReports`
```json
{
  "failEventReports": [
    {"event": "ABNORMAL_BEHAVIOUR", "failureCode": "OTHER"}
  ]
}
```

---

### 4.2 All Events Unsupported

**Command:** `./test_api.sh anatype`

**Description:** All events use unsupported exptAnaType.

**Expected Result:** `400 Bad Request` with cause `ALL_EVENTS_UNSUPPORTED`

---

## 5. Phase 2B Test Cases

### 5.1 PERIODIC without repPeriod

**Command:** `./test_api.sh periodic`

**Expected:** `400 Bad Request`

### 5.2 Valid evtReq

**Command:** `./test_api.sh evtreq`

**Expected:** `201 Created`

---

## 6. Manual Testing

### Create with evtReq

```bash
curl -X POST http://127.0.0.1:8080/nnwdaf-eventssubscription/v1/subscriptions \
  -H "Content-Type: application/json" \
  -d '{
    "eventSubscriptions": [{
      "event": "ABNORMAL_BEHAVIOUR",
      "tgtUe": {"anyUe": true},
      "excepRequs": [{"excepId": "SUSPICION_OF_DDOS_ATTACK"}],
      "dnns": ["internet"]
    }],
    "evtReq": {
      "notifMethod": "PERIODIC",
      "repPeriod": 60
    },
    "notificationURI": "http://localhost:9090/callback"
  }'
```

---

## 7. Troubleshooting

| Issue | Solution |
|-------|----------|
| HTTP 000 | Start the server first |
| Connection refused | Check `netstat -tlnp \| grep 8080` |
| Build errors | Run `go mod tidy` |
