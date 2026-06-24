# NWDAF

Network Data Analytics Function (NWDAF) implementation based on 3GPP TS 29.520.

## Prerequisites

- Go 1.25.5
- MongoDB (optional — NWDAF falls back to in-memory without it)

Current runtime notes:

- SBI server runs over HTTP only.
- `nrfUri` is a reserved config field; NRF registration is not wired yet.
- Metrics server is not wired yet.

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
