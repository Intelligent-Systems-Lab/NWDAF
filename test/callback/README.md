# Notification Callback Test Server

Simple Python server to test NWDAF subscription notifications.

## Quick Start

```bash
cd test/callback

# Initialize and run
uv run callback_server.py 9090
```

## Test Flow

### Terminal 1: Start callback server
```bash
cd test/callback
uv run callback_server.py 9090
```

### Terminal 2: Start NWDAF server
```bash
./bin/nwdaf --config config/nwdafcfg.yaml
```

### Terminal 3: Create periodic subscription
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
    "evtReq": {
      "notifMethod": "PERIODIC",
      "repPeriod": 10
    }
  }'
```

## Expected Output

Callback server will show notifications every 10 seconds:

```
============================================================
[20:45:30] 📩 Notification Received
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
