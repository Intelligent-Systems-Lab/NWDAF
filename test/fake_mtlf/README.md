# Fake MTLF Server

Mock NWDAF(MTLF) server for ML Model Provisioning E2E testing.

## Usage

```bash
# Start server (default: port 8082)
uv run fake_mtlf_server.py 8082
```

## Endpoints

### `POST /nnwdaf-mlmodelprovision/v1/subscriptions`
Accepts ML Model Provision subscription requests per TS 29.520 §5.4.

**Request**: `NwdafMLModelProvSubsc`
**Response**: 201 Created with subscription ID

After accepting subscription, sends async notification to `notifUri` with model URL.
