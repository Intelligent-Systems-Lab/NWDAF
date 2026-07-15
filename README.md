# NWDAF

Network Data Analytics Function (NWDAF) implementation based on 3GPP TS 29.520.

## Prerequisites

- Go 1.25.5
- MongoDB (optional — NWDAF falls back to in-memory without it)

Current runtime notes:

- The main SBI server supports both HTTP and HTTPS, selected through runtime
  configuration.
- `nrfUri` is required. NWDAF registers its Events Subscription service before
  starting owned listeners, retries temporary NRF transport/server failures
  until cancellation, and attempts bounded deregistration before SBI shutdown.
  Phase 0 targets an OAuth-disabled NRF; redirect handling, OAuth/certificate
  support, discovery, and NFUpdate heartbeat remain explicit later work.
- The main SBI server currently mounts free5GC's inbound request metrics
  middleware, and the NRF NFManagement generated client installs the outbound
  SBI metrics hook. Collectors, hooks for the remaining outbound clients, and
  the separately owned `/metrics` server are not wired yet; completing that
  free5GC metrics pattern remains an independent, default-disabled supporting
  workstream and does not block the NRF path.

## Build

```bash
# Build
make build

# Run
./bin/nwdaf --config config/nwdafcfg.yaml
```

## Testing

```bash
# Unit tests
go test ./...

# Lint
make lint
```

## License

Apache-2.0
