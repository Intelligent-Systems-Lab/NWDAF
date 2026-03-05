# NWDAF

Network Data Analytics Function (NWDAF) implementation based on 3GPP TS 29.520.

## Prerequisites

- Go 1.21+
- MongoDB (optional — NWDAF falls back to in-memory without it)

See [docs/testing.md](docs/testing.md) for MongoDB setup instructions.

## Build

```bash
# Build
make build

# Run
./bin/nwdaf --config config/nwdafcfg.yaml
```

## Testing

See [docs/testing.md](docs/testing.md) for detailed testing instructions.

```bash
# Unit tests
go test ./... -v

# API tests (requires server running)
./test/scripts/test_api.sh all
```

## License

Apache-2.0
