# Testing Guide

This document describes how to run tests for the NWDAF EventSubscription service.

## Prerequisites

- Go 1.21+
- `jq` (for JSON parsing in shell scripts)
- `curl`

## 1. Starting the Server

Before running API tests, you must start the NWDAF server.

### Build and Run

```bash
# Build the binary
make build

# Run with default config
./bin/nwdaf --config config/nwdafcfg.yaml

# Or run directly
make run
```

### Expected Output

```
INFO NWDAF
INFO NWDAF version: v0.1.0
INFO Start SBI server (bindingAddr: 127.0.0.1:8080)
```

The server listens on `http://127.0.0.1:8080` by default.

---

## 2. Go Unit Tests

Unit tests can be run without starting the server.

### Run All Unit Tests

```bash
go test ./... -v
```

### Run Specific Package Tests

```bash
# Context tests (subscription CRUD)
go test ./internal/context/... -v

# Processor tests (validation logic)
go test ./internal/sbi/processor/... -v
```

### Test Coverage

| Package | Test File | Description |
|---------|-----------|-------------|
| `internal/context` | `context_test.go` | Subscription storage CRUD operations |
| `internal/sbi/processor` | `eventssubscription_test.go` | Validation logic for ABNORMAL_BEHAVIOUR |

---

## 3. API Integration Tests

These tests require the server to be running.

### Quick Start

```bash
# Terminal 1: Start server
./bin/nwdaf --config config/nwdafcfg.yaml

# Terminal 2: Run tests
./test/scripts/test_api.sh all
```

### Test Script Usage

```bash
./test/scripts/test_api.sh [command]
```

| Command | Description |
|---------|-------------|
| `all` | Run all tests (default) |
| `create` | Test valid subscription creation |
| `mutual` | Test excepRequs/exptAnaType mutual exclusion |
| `anyue` | Test anyUe missing required fields |
| `unsupported` | Test unsupported event type rejection |
| `delete <id>` | Test subscription deletion |

---

## 4. Test Cases

### 4.1 Create Valid Subscription

**Command:** `./test_api.sh create`

**Description:** Creates a valid ABNORMAL_BEHAVIOUR subscription with:
- `anyUe: true`
- `excepRequs: SUSPICION_OF_DDOS_ATTACK`
- `dnns: ["internet"]`

**Expected Result:** `201 Created`

---

### 4.2 Mutual Exclusion Validation

**Command:** `./test_api.sh mutual`

**Description:** Tests that `excepRequs` and `exptAnaType` cannot be provided together.

**Request:**
```json
{
  "excepRequs": [{"excepId": "SUSPICION_OF_DDOS_ATTACK"}],
  "exptAnaType": "COMMUN"
}
```

**Expected Result:** `400 Bad Request`

---

### 4.3 anyUe Missing Required Fields

**Command:** `./test_api.sh anyue`

**Description:** Tests that when `anyUe=true` with communication-related analytics, at least one of `networkArea`, `appIds`, `dnns`, or `snssais` is required.

**Expected Result:** `400 Bad Request`

---

### 4.4 Unsupported Event Type

**Command:** `./test_api.sh unsupported`

**Description:** Tests that only `ABNORMAL_BEHAVIOUR` event type is supported. Other event types (e.g., `UE_MOBILITY`) should be rejected.

**Expected Result:** `400 Bad Request` with cause `UNSUPPORTED_EVENT`

---

### 4.5 Delete Subscription

**Command:** `./test_api.sh delete <subscriptionId>`

**Description:** Deletes an existing subscription by ID.

**Expected Result:** `204 No Content`

---

## 5. Manual Testing with curl

### Create Subscription

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
    "notificationURI": "http://localhost:9090/callback"
  }'
```

### Delete Subscription

```bash
curl -X DELETE http://127.0.0.1:8080/nnwdaf-eventssubscription/v1/subscriptions/{subscriptionId}
```

---

## 6. Troubleshooting

### HTTP 000 Response

This means the server is not running or not reachable.

**Solution:** Start the server first:
```bash
./bin/nwdaf --config config/nwdafcfg.yaml
```

### Connection Refused

Check if the server is listening on the expected port:
```bash
netstat -tlnp | grep 8080
```

### Build Errors

Ensure dependencies are installed:
```bash
go mod tidy
```
