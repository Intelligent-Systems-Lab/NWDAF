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

# Run context tests only
go test ./internal/context/... -v

# Run consumer tests only
go test ./internal/sbi/consumer/... -v
```

### 2.2 Processor Tests

#### Subscription Validation (`eventssubscription_test.go`)

| Test Function | Description |
|---------------|-------------|
| `TestValidateSupportedEvent` | Event type validation (UE_COMMUNICATION supported) |
| `TestValidateUeCommunication` | UE_COMMUNICATION validation (tgtUe, supis/intGroupIds) |
| `TestValidateEvtReq` | evtReq validation (PERIODIC/repPeriod/maxReportNbr) |
| `TestCollectFailEventReports` | failEventReports collection |
| `TestValidateEventTargetPeriod` | startTs/endTs validation |
| `TestApplyAndValidateDefaults` | Default value handling (THRESHOLD/PERIODIC validation) |

#### SMF Notification Handling (`smf_notify_test.go`)

Tests SMF event processing and TrafficData enrichment.

| Test Function | Description |
|---------------|-------------|
| `TestHandleSmfNotification_Basic` | Basic SMF notification processing |
| `TestHandleSmfNotification_EnrichTrafficData` | Enrich SUPI/DNN/SNSSAI/RatType |
| `TestHandleSmfNotification_MultipleEvents` | Multiple events in one notification |
| `TestHandleSmfNotification_MissingSupi` | Missing SUPI handling (skip event) |
| `TestHandleSmfNotification_NoMatchingBucket` | No bucket → no error, no creation |
| `TestHandleSmfNotification_EnrichMultipleTrafficData` | Enrich all IPs in bucket |

#### UPF Notification Handling (`upf_notify_test.go`)

Tests UPF data point storage using bucket-based architecture.

| Test Function | Description |
|---------------|-------------|
| `TestHandleUpfNotification_Basic` | Basic notification → creates TrafficData with RawUpfData |
| `TestHandleUpfNotification_MultipleItems` | Multiple IP addresses in one notification |
| `TestHandleUpfNotification_DataAccumulation` | Multiple notifications → RawUpfData accumulates |
| `TestHandleUpfNotification_MissingCorrelationId` | Missing correlationId → skip (no error) |
| `TestHandleUpfNotification_UpdateSmfSubscription` | Updates SmfSubscription.LastUpdate |
| `TestHandleUpfNotification_WithMetadata` | DNN/SNSSAI/RatType/SUPI storage |
| `TestHandleUpfNotification_ThroughputMeasurement` | Throughput strings stored correctly |

#### Data Collection Tests (`data_collection_test.go`)

Tests data collection triggering and resource management.

| Test Function | Description |
|---------------|-------------|
| `TestTriggerTargetDataCollection_ResourceReuse` | Verifies SMF subscription reuse (target mapping and ref counting) |

### 2.3 Notifier Tests

Location: `internal/notifier/notifier_test.go`

| Test Function | Description |
|---------------|-------------|
| `TestShouldContinue_MaxReportNbrLimit` | Scheduler stops at maxReportNbr |
| `TestShouldContinue_MonDurExpiry` | Scheduler stops when monDur expires |
| `TestBuildNotification_NotifCorrId` | notifCorrId in notification |

### 2.4 Context Tests

Location: `internal/context/`

#### Traffic Data Tests (`traffic_data_test.go`)

**TrafficDataBucket Tests**
| Test Function | Description |
|---------------|-------------|
| `TestTrafficDataBucket_Basic` | Bucket creation with correlationId |
| `TestTrafficDataBucket_GetOrCreate` | Get or create by IP address |
| `TestTrafficDataBucket_Get` | Get existing TrafficData |
| `TestTrafficDataBucket_GetAll` | Get all TrafficData in bucket |
| `TestTrafficDataBucket_Delete` | Delete by IP address |
| `TestTrafficDataBucket_Concurrent` | 100 goroutines concurrent access |

**TrafficData Tests**
| Test Function | Description |
|---------------|-------------|
| `TestTrafficData_EnrichWithSupi` | SUPI enrichment (first wins) |
| `TestTrafficData_AppendDataPoint` | Append RawUpfData with timestamp |

**NWDAFContext TrafficBucket Tests**
| Test Function | Description |
|---------------|-------------|
| `TestNWDAFContext_TrafficBucket` | Get/Create/Delete bucket |
| `TestNWDAFContext_GetOrCreateTrafficData` | Get or create TrafficData |
| `TestNWDAFContext_GetAllTrafficDataForCorrelation` | Get all data for correlationId |

**SmfSubscription Tests**
| Test Function | Description |
|---------------|-------------|
| `TestNWDAFContext_SmfSubscription` | Get/Create/Update/Delete |
| `TestSmfSubscription_ReferenceCount` | Reference counting (add/remove) |
| `TestSmfSubscription_UpdateLastSeen` | LastUpdate timestamp |
| `TestSmfSubscription_ValidateInvariant` | RefCount == len(NwdafSubIds) |

**Unified Query Tests (nwdafSubId → correlationIds → data)**
| Test Function | Description |
|---------------|-------------|
| `TestGetCorrelationIdsByNwdafSubId` | Get correlationIds by nwdafSubId |
| `TestGetCorrelationIdsByNwdafSubId_Empty` | Non-existent nwdafSubId → nil |
| `TestGetTrafficBucketsByNwdafSubId` | Get buckets by nwdafSubId |
| `TestGetTrafficDataByNwdafSubId` | Get all TrafficData by nwdafSubId |
| `TestGetTrafficDataByNwdafSubId_Empty` | Non-existent → nil |
| `TestGetTrafficDataByNwdafSubId_MultipleCorrelations` | Multiple correlationIds |
| `TestGetTrafficDataByNwdafSubId_NoBucket` | Resource exists but no bucket → nil |

#### Other Context Tests

| Test File | Tests |
|-----------|-------|
| `context_test.go` | Subscription CRUD operations |

#### Thread Safety Testing

Run tests with race detector to verify concurrent access:
```bash
go test ./internal/... -race -v
```

### 2.5 Consumer Tests

Location: `internal/sbi/consumer/consumer_test.go`

| Test Function | Description |
|---------------|-------------|
| `TestNewConsumer` | Consumer initialization |
| `TestConsumerContext` | Context accessor |
| `TestSmfServiceHTTPClient` | HTTP client caching |
| `TestExtendedEventSubscription` | UPF event subscription model |

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

## 6. E2E Integration Test (ML-Based Analytics)

This test validates the complete ML-based analytics flow:
- NWDAF subscribes to MTLF for ML Model Provisioning
- MTLF notifies NWDAF with ML model URL
- NWDAF initializes model with ML Service
- Analytics use ML prediction (returns 0 confidence when insufficient data)

### 6.1 Test Architecture

```
┌──────────────────┐                     ┌──────────────────────┐
│    Consumer      │◄────Notification────│       NWDAF          │
│  Callback Server │    (ML-based)       │     :8080            │
│     :9091        │                     │                      │
└──────────────────┘                     └──────────┬───────────┘
                                                    │
                              ┌─────────────────────┼─────────────────────┐
                              │                     │                     │
                    ┌─────────▼──────────┐ ┌───────▼────────┐ ┌──────────▼─────────┐
                    │   Fake MTLF        │ │  ML Service    │ │   Fake SMF+UPF     │
                    │     :8082          │ │    :9090       │ │      :8081         │
                    │                    │ │                │ │                    │
                    │ - ML model URL     │ │ - /model/load  │ │ - UPF notifications│
                    │   notification     │ │ - /predict     │ │   (traffic data)   │
                    └────────────────────┘ └────────────────┘ └────────────────────┘
```

### 6.2 Prerequisites

1. **NWDAF-ML-Service** project available and runnable
2. Config has MTLF and ML Service enabled:
   ```yaml
   dataCollection:
     mtlf:
       enabled: true
       endpoints:
         - http://127.0.0.1:8082
       notifUri: http://127.0.0.1:8080/mlmodel-notify
     mlService:
       enabled: true
       endpoint: http://127.0.0.1:9090
   ```

### 6.3 Start Test (5 Terminals)

#### Terminal 1: Fake MTLF Server
```bash
cd test/fake_mtlf
uv run fake_mtlf_server.py 8082
```

#### Terminal 2: ML Inference Service
```bash
cd /path/to/NWDAF-ML-Service
uv run python run.py
```

#### Terminal 3: Fake SMF+UPF Server
```bash
cd test/fake_smf_upf
uv run fake_smf_upf_server.py 8081
```

#### Terminal 4: Consumer Callback Server
```bash
cd test/callback
uv run callback_server.py 9091
```

#### Terminal 5: NWDAF
```bash
./bin/nwdaf --config config/nwdafcfg.yaml
```

### 6.4 Send Test Subscription (Group ID)

```bash
curl -X POST http://127.0.0.1:8080/nnwdaf-eventssubscription/v1/subscriptions \
  -H "Content-Type: application/json" \
  -d '{
    "eventSubscriptions": [{
      "event": "UE_COMMUNICATION",
      "tgtUe": {"intGroupIds": ["group-test-001"]}
    }],
    "evtReq": {"notifMethod": "PERIODIC", "repPeriod": 10},
    "notificationURI": "http://127.0.0.1:9091/notify"
  }'
```

### 6.5 Verification Checklist

| Step | Expected Log | Location |
|------|--------------|----------|
| MTLF Subscription | `MTLF subscription created` | NWDAF |
| Model Notification | `Received ML Model Provision notification` | NWDAF |
| Model Init | `ML model initialized successfully` | NWDAF |
| Prediction | `Using ML-based analytics` OR `returning 0 confidence` | NWDAF |
| Notification | `Notification #N sent successfully` | NWDAF |

### 6.6 Cleanup

Press `Ctrl+C` to stop all 5 services.

---

## 7. Test Directory Structure

```
test/
├── callback/                 # Consumer notification callback
│   ├── callback_server.py    # HTTP callback server
│   ├── pyproject.toml        # uv project configuration
│   └── README.md
├── fake_smf_upf/            # Fake SMF+UPF for E2E testing
│   ├── fake_smf_upf_server.py
│   ├── pyproject.toml
│   └── README.md
└── scripts/
    └── test_api.sh          # API integration test script
```

---

## 8. Quick Test Commands

| Test Type | Command |
|-----------|---------|
| Unit Tests | `go test ./internal/... -v` |
| Race Detection | `go test ./internal/... -race -v` |
| API Tests | `./test/scripts/test_api.sh all` |
| E2E (4 terminals) | See Section 6.3 |

---

## 9. Error Code Reference

| HTTP | Cause | Description |
|------|-------|-------------|
| 400 | `INVALID_REQUEST` | Request format error |
| 400 | `ALL_EVENTS_UNSUPPORTED` | All events unsupported |
| 400 | `BOTH_STAT_PRED_NOT_ALLOWED` | Statistics + prediction not allowed |
| 400 | `UNSUPPORTED_NOTIF_METHOD` | Unknown notificationMethod |
| 404 | `SUBSCRIPTION_NOT_FOUND` | Subscription does not exist |
| 501 | `THRESHOLD_NOT_IMPLEMENTED` | THRESHOLD notifMethod not yet implemented |

---

## 10. Group ID Subscription Formats Reference

### 10.1 NWDAF Subscription Request (Consumer -> NWDAF)

When subscribing to NWDAF for a group of UEs, use the `intGroupIds` field within `tgtUe`.

**JSON Payload**:
```json
{
  "eventSubscriptions": [{
    "event": "UE_COMMUNICATION",
    "tgtUe": {
      "intGroupIds": ["group-01", "group-02"]
    }
  }],
  "notificationURI": "http://127.0.0.1:9090/notify",
  "evtReq": {
    "notifMethod": "PERIODIC", 
    "repPeriod": 10
  }
}
```

### 10.2 SMF Subscription Request (NWDAF -> SMF)

NWDAF translates the group subscription and sends a request to the SMF using the `groupId` field.

**JSON Payload**:
```json
{
  "groupId": "group-01",
  "notifUri": "http://127.0.0.1:8080/collector/notify",
  "notifId": "4021c603-5ba2-4135-8be0-d51f915c6394",
  "eventSubs": [
    {
      "event": "UPF_EVENT",
      "upfEvents": [
        {
          "type": "USER_DATA_USAGE_MEASURES",
          "measurementTypes": [
            "VOLUME_MEASUREMENT",
            "THROUGHPUT_MEASUREMENT"
          ],
          "granularityOfMeasurement": "PER_SESSION"
        }
      ],
      "bundlingAllowed": true,
      "bundledEventNotifyUri": "http://127.0.0.1:8080/collector/upf-notify"
    }
  ],
  "notifMethod": "PERIODIC",
  "repPeriod": 10
}
```

**Verification**:
- **Source Code**: `internal/sbi/consumer/models.go` defines `ExtendedNsmfEventExposure` with `json:"groupId"`.