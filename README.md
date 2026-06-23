# NWDAF

Network Data Analytics Function (NWDAF) implementation based on 3GPP TS 29.520.

## Prerequisites

- Go 1.21+
- MongoDB (optional — NWDAF falls back to in-memory without it)

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
