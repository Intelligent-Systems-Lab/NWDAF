# Fake SMF+UPF Server

A test server that simulates both SMF and UPF behavior for testing NWDAF data collection.

## Features

- **SMF Subscription Endpoint**: Accepts `Nsmf_EventExposure` subscription requests
- **UPF Notification**: Sends periodic `USER_DATA_USAGE_MEASURES` notifications
- **Traffic Simulation**: Varying traffic patterns to test rule-based analytics

## Usage

```bash
# Start server on default port 8081
uv run fake_smf_upf_server.py

# Or specify port
uv run fake_smf_upf_server.py 8082
```

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/nsmf-event-exposure/v1/subscriptions` | Create subscription |
| DELETE | `/nsmf-event-exposure/v1/subscriptions/{subId}` | Delete subscription |

## Notification Behavior

- Sends UPF notifications every **10 seconds** to the `bundledEventNotifyUri`
- Traffic volume varies randomly (100KB-700KB per notification)
- Every 5th notification simulates a **traffic spike** (10x volume)

## Testing Flow

1. Start this server: `uv run fake_smf_upf_server.py 8081`
2. Start callback server: `cd ../callback && uv run callback_server.py 9090`
3. Start NWDAF: `./bin/nwdaf --config config/nwdafcfg.yaml`
4. Send subscription: `./test/scripts/test_api.sh create_ue_comm`
