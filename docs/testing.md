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
- MongoDB (optional, for time-series storage; see [§6.1](#61-prerequisites) for setup)

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
| `TestDataCollectionTarget_Identifier` | Identifier() returns correct format |
| `TestDataCollectionTarget_OriginalGroupIdTracking` | OriginalGroupId preserved after resolution |
| `TestTriggerTargetDataCollection_WithOriginalGroupId` | Group → multiple SUPI subscriptions with tracking |
| `TestTriggerTargetDataCollection_MixedSupiAndGroup` | Mixed SUPI and Group-resolved targets |

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
| `TestAppendDataPoint_RingBuffer_ExactCap` | 50 pts at cap — oldest not dropped |
| `TestAppendDataPoint_RingBuffer_OverCap` | 60 pts — oldest 10 dropped, newest 50 kept |
| `TestAppendDataPoint_RingBuffer_LastUpdate` | LastUpdate tracks most recent timestamp |

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

**MongoDB Query Tests (`db_query_test.go`)**
| Test Function | Description |
|---------------|-------------|
| `TestReverseRecords_Empty` | Reverse of empty slice — no panic |
| `TestReverseRecords_Single` | Single element unchanged |
| `TestReverseRecords_Multiple` | DESC→ASC reverse correctness (timestamps + values) |
| `TestReverseRecords_Even` | Even-length slice fully reversed |
| `TestIsMongoAvailable_ReturnsFalseWithoutClient` | Returns false when `mongoapi.Client` is nil |
| `TestQueryTrafficByCorrelationId_NoMongo` | Returns `nil, nil` when MongoDB unavailable |
| `TestQueryTrafficByMultipleCorrelationIds_NoMongo` | Returns `nil, nil` when MongoDB unavailable |
| `TestQueryTrafficByMultipleCorrelationIds_EmptyIds` | Returns `nil, nil` for empty ID slice |
| `TestQueryTrafficInTimeRange_NoMongo` | Returns `nil, nil` when MongoDB unavailable |
| `TestQueryTrafficInTimeRange_EmptyIds` | Returns `nil, nil` for empty ID slice |

#### GroupResolver Tests (`group_resolver_test.go`)

Tests Group ID → SUPI resolution per TS 23.502 §4.15.4.5.2.

| Test Function | Description |
|---------------|-------------|
| `TestNewGroupResolver_NilConfig` | Nil config handling |
| `TestNewGroupResolver_EmptyConfig` | Empty config handling |
| `TestNewGroupResolver_ValidConfig` | Valid config loading |
| `TestNewGroupResolver_SkipsEmptyGroupId` | Skip empty GroupId entries |
| `TestGroupResolver_ResolveGroupId_Success` | Successful Group ID resolution |
| `TestGroupResolver_ResolveGroupId_NotFound` | Non-existent Group ID error |
| `TestGroupResolver_ResolveGroupId_EmptyMembers` | Empty member list error |
| `TestGroupResolver_HasGroup` | Check if Group exists |
| `TestGroupResolver_GetAllGroups` | Get all registered Group IDs |
| `TestNWDAFContext_GroupResolver` | Context integration test |

#### Other Context Tests

| Test File | Tests |
|-----------|-------|
| `context_test.go` | Subscription CRUD operations |

#### Thread Safety Testing

Run tests with race detector to verify concurrent access:
```bash
go test ./internal/... -race -v
```

### 2.5 Factory Tests

Location: `pkg/factory/`

**ModelParams Tests (`model_params_test.go`)**
| Test Function | Description |
|---------------|-------------|
| `TestModelParams_Defaults` | Zero-value returns default si=10/iw=30/ow=5 |
| `TestModelParams_ExplicitValues` | Explicit values returned as-is |
| `TestModelParams_QueryLookback_Defaults` | 0-value → 10s × 30pts = 300s |
| `TestModelParams_QueryLookback_Custom` | 5s × 20pts = 100s |
| `TestModelParams_QueryLookback_ZeroInterval` | Zero interval falls back to 10s |
| `TestModelParams_QueryLookback_ZeroInputWindow` | Zero inputWindow falls back to 30pts |
| `TestModelParams_NegativeValues_UseDefaults` | Negative values fall through to defaults |
| `TestAnalyticsConfig_NilUeCommunication` | nil UeCommunication field safe |

### 2.6 Consumer Tests

Location: `internal/sbi/consumer/`

#### General Tests (`consumer_test.go`)

| Test Function | Description |
|---------------|-------------|
| `TestNewConsumer` | Consumer initialization |
| `TestConsumerContext` | Context accessor |
| `TestSmfServiceHTTPClient` | HTTP client caching |
| `TestExtendedEventSubscription` | UPF event subscription model |

#### DaisyClient Tests (`daisy_service_test.go`)

| Test Function | Description |
|---------------|-------------|
| `TestNewDaisyClient` | Client initialization + timeout verification |
| `TestTriggerTraining_Success` | POST payload correctness + 200 OK handling |
| `TestTriggerTraining_WithAutoTID` | Auto-generate TID when not provided |
| `TestTriggerTraining_ServerError` | 500 error handling |
| `TestTriggerTraining_ConnectionError` | Connection failure handling |

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

## 6. E2E Integration Test (MongoDB Data Layer)

Verifies that analytics read from MongoDB Time Series Collection instead of in-memory store.

### 6.1 Prerequisites

- MongoDB running and accessible (URI configured in `nwdafcfg.yaml`)
- Fake SMF+UPF server providing traffic data

#### Starting MongoDB

**Option A — Docker (recommended, version-independent):**
```bash
docker run -d --name mongodb -p 27017:27017 mongo:8
```

**Option B — Native install (Ubuntu):**

The required steps vary by Ubuntu version (GPG key import + apt source setup).
Refer to the official guide and select your Ubuntu release:
https://www.mongodb.com/docs/manual/tutorial/install-mongodb-on-ubuntu/

Once installed:
```bash
sudo systemctl start mongod
```

**Verify connection:**
```bash
mongosh --eval "db.runCommand({ ping: 1 })"
```

### 6.2 Config (`config/nwdafcfg.yaml`)

```yaml
mongodb:
  name: free5gc
  url: mongodb://localhost:27017

analytics:
  ueCommunication:
    samplingInterval: 10   # UPF report period (s)
    inputWindow: 30        # Points fed to ML model
    outputWindow: 5        # Prediction steps
```

### 6.3 Start Test

```bash
# T1: Fake SMF+UPF
cd test/fake_smf_upf && uv run fake_smf_upf_server.py

# T2: NWDAF (debug log to see data source)
./bin/nwdaf --config config/nwdafcfg.yaml

# T3: Consumer callback
cd test/callback && uv run callback_server.py 9091
```

### 6.4 Create Subscription

```bash
curl -s -X POST http://127.0.0.1:8080/nnwdaf-eventssubscription/v1/subscriptions \
  -H 'Content-Type: application/json' \
  -d '{
    "notificationURI": "http://127.0.0.1:9091/notify",
    "eventSubscriptions": [{"event": "UE_COMMUNICATION",
      "tgtUe": {"supis": ["imsi-208930000000001"]}}],
    "evtReq": {"notifMethod": "PERIODIC", "repPeriod": 30}
  }'
```

### 6.5 Verify MongoDB Data

```bash
# Wait ~30s for UPF data to accumulate, then:
mongosh --eval "db.getSiblingDB('free5gc').getCollection('nwdaf.upfTrafficData').find().sort({timestamp:-1}).limit(3)"
```

### 6.6 Verification Checklist

| Step | Expected Log (level=debug) | What It Confirms |
|------|---------------------------|------------------|
| UPF data write | `InsertOne` to `nwdaf.upfTrafficData` | Data stored in MongoDB |
| ML prediction | `Using X MongoDB records for ML prediction` | Analytics reads from DB |
| Fallback (no MongoDB) | `Using in-memory data for ML prediction` | Fallback works |

### 6.7 Database Maintenance

```bash
# Count records in collection
mongosh --quiet --eval "db.getSiblingDB('free5gc').getCollection('nwdaf.upfTrafficData').countDocuments({})"

# Clear UPF data only (recommended between test runs)
mongosh --quiet --eval "db.getSiblingDB('free5gc').getCollection('nwdaf.upfTrafficData').drop()"

# Clear entire database
mongosh --quiet --eval "db.getSiblingDB('free5gc').dropDatabase()"
```

> [!NOTE]
> After NWDAF restart without clearing the database, **new subscriptions get new correlationIds**
> (sequential: `corr-1`, `corr-2`, ...) which reset to `corr-1` on restart. Old data with the same
> correlationId will be picked up again in tests — this is expected behaviour in test environments.

---

## 7. E2E Integration Test (ML-Based Analytics)

This test validates the complete ML-based analytics flow:
- NWDAF uses static model URL (MTLF disabled) or subscribes to MTLF
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
                                          ┌─────────┼─────────────────────┐
                                          │                               │
                                ┌─────────▼──────────┐       ┌───────────▼───────────┐
                                │   ML Service        │       │   Fake SMF+UPF        │
                                │     :9090           │       │      :8081            │
                                │                     │       │                       │
                                │  - /model/load      │       │  - UPF notifications  │
                                │  - /predict          │       │    (traffic data)     │
                                └─────────────────────┘       └───────────────────────┘
```

### 6.2 Prerequisites

1. **NWDAF-ML-Service** project available and runnable
2. Config uses static model URL (MTLF disabled by default):
   ```yaml
   mtlf:
     enabled: false
     staticModelUrl: file:///app/ml/artifacts/tcn_model.pth
   mlService:
     enabled: true
     endpoint: http://127.0.0.1:9090
   ```

### 6.3 Start Test (4 Terminals)

#### Terminal 1: ML Inference Service
```bash
cd /path/to/NWDAF-ML-Service
uv run python run.py
```

#### Terminal 2: Fake SMF+UPF Server
```bash
cd test/fake_smf_upf
uv run fake_smf_upf_server.py 8081
```

#### Terminal 3: Consumer Callback Server
```bash
cd test/callback
uv run callback_server.py 9091
```

#### Terminal 4: NWDAF
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
    "evtReq": {"notifMethod": "PERIODIC", "repPeriod": 5},
    "notificationURI": "http://127.0.0.1:9091/notify"
  }'
```

### 6.5 Verification Checklist

| Step | Expected Log | Location |
|------|--------------|----------|
| Static Model Init | `ML model initialized successfully` | NWDAF |
| Prediction | `Using ML-based analytics` OR `returning 0 confidence` | NWDAF |
| Notification | `Notification #N sent successfully` | NWDAF |

### 6.6 Cleanup

Press `Ctrl+C` to stop all 4 services.

---

## 7. E2E Integration Test (MTLF Training via Daisy)

This test validates the MTLF training trigger flow:
- NWDAF starts with `daisy.enabled: true`
- After `triggerDelay` seconds, NWDAF POSTs training task to Daisy master
- Daisy master coordinates federated learning across clients
- POST blocks until training completes (HTTP 200 = success)

### 7.1 Test Architecture

```
┌──────────────────────┐
│       NWDAF          │
│     :8080            │
│                      │
│ (Startup → Delay →   │
│  POST /publish_task) │
└──────────┬───────────┘
           │ HTTP POST (blocks until training completes)
┌──────────▼───────────┐
│   Daisy Master       │
│   gRPC :8887         │
│   REST :9887         │
│   (FedAvg strategy)  │
└──────────┬───────────┘
     ┌─────┴─────┐
┌────▼────┐ ┌────▼────┐
│ Client0 │ │ Client1 │
│  :10087 │ │  :10088 │
└─────────┘ └─────────┘
```

### 7.2 Prerequisites

1. **Daisy FL** framework installed (`.agent/daisy/`)
2. Config:
   ```yaml
   daisy:
     enabled: true
     endpoint: http://127.0.0.1:9887
     triggerDelay: 10
   ```

### 7.3 Start Test (2 Terminals)

#### Terminal 1: Deploy Daisy
```bash
cd .agent/daisy/examples/01_quickstart_pytorch
python deploy.py --init_model
```

#### Terminal 2: NWDAF
```bash
make build
./bin/nwdaf --config config/nwdafcfg.yaml
```

### 7.4 Verification Checklist

| Step | Expected Log | Location |
|------|--------------|----------|
| Startup | `MTLF training scheduled in 10 seconds` | NWDAF (MTLF) |
| Trigger | `Triggering MTLF training via Daisy: endpoint=...` | NWDAF (MTLF) |
| Training | FL rounds running | Daisy Master |
| Complete | `MTLF training completed successfully` | NWDAF (MTLF) |

### 7.5 Shutdown Cancellation Test

Start NWDAF and press `Ctrl+C` within `triggerDelay` seconds.

**Expected**: `MTLF training canceled (shutdown)` in NWDAF log.

### 7.6 Cleanup

```bash
# Stop Daisy nodes
cd .agent/daisy/examples/01_quickstart_pytorch
python shutdown.py
```

---

## 8. Test Directory Structure

```
test/
├── callback/                 # Consumer notification callback
│   ├── callback_server.py    # HTTP callback server
│   ├── pyproject.toml        # uv project configuration
│   └── README.md
├── fake_mtlf/               # Fake MTLF server (optional, for dynamic model provisioning)
│   ├── fake_mtlf_server.py
│   ├── pyproject.toml
│   └── README.md
├── fake_smf_upf/            # Fake SMF+UPF for E2E testing
│   ├── fake_smf_upf_server.py
│   ├── pyproject.toml
│   └── README.md
└── scripts/
    └── test_api.sh          # API integration test script
```

---

## 9. Quick Test Commands

| Test Type | Command |
|-----------|---------|
| Unit Tests | `go test ./internal/... ./pkg/factory/... -v` |
| Ring Buffer Tests | `go test ./internal/context/... -v -run RingBuffer` |
| DB Query Tests | `go test ./internal/context/... -v -run Query\|Reverse\|Mongo` |
| ModelParams Tests | `go test ./pkg/factory/... -v -run ModelParams` |
| Race Detection | `go test ./internal/... -race -v` |
| API Tests | `./test/scripts/test_api.sh all` |
| E2E MongoDB Data Layer (3 terminals) | See Section 6.3 |
| E2E ML Analytics (4 terminals) | See Section 7.3 |
| E2E Daisy Training (2 terminals) | See Section 8.3 |

---

## 10. Error Code Reference

| HTTP | Cause | Description |
|------|-------|-------------|
| 400 | `INVALID_REQUEST` | Request format error |
| 400 | `ALL_EVENTS_UNSUPPORTED` | All events unsupported |
| 400 | `BOTH_STAT_PRED_NOT_ALLOWED` | Statistics + prediction not allowed |
| 400 | `UNSUPPORTED_NOTIF_METHOD` | Unknown notificationMethod |
| 404 | `SUBSCRIPTION_NOT_FOUND` | Subscription does not exist |
| 501 | `THRESHOLD_NOT_IMPLEMENTED` | THRESHOLD notifMethod not yet implemented |

---

## 11. Group ID Subscription Formats Reference

### 11.1 NWDAF Subscription Request (Consumer -> NWDAF)

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

### 11.2 SMF Subscription Request (NWDAF -> SMF)

Per **TS 23.502 §4.15.4.5.2**: NWDAF resolves Group ID to SUPI list before subscribing to SMF.
Each SUPI receives its own subscription request.

**Group ID Resolution Flow**:
```
Consumer Request:       intGroupIds: ["group-01"]
                              ↓
NWDAF GroupResolver:    group-01 → [imsi-001, imsi-002, imsi-003]
                              ↓
SMF Subscriptions:      POST /subscriptions { supi: "imsi-001" }
                        POST /subscriptions { supi: "imsi-002" }
                        POST /subscriptions { supi: "imsi-003" }
```

**JSON Payload (per SUPI)**:
```json
{
  "supi": "imsi-123456789012345",
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

**Key Points**:
- `supi` field is always present (no `groupId` to SMF)
- `OriginalGroupId` is tracked internally for analytics aggregation
- Configuration: `groupMembership` in `config/nwdafcfg.yaml`

**Verification**:
- **Source Code**: `internal/sbi/consumer/models.go` defines `ExtendedNsmfEventExposure` (SUPI-only).
- **GroupResolver**: `internal/context/group_resolver.go` handles Group ID → SUPI resolution.