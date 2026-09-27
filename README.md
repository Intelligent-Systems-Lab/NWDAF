# NWDAF

Network Data Analytics Function based on 3GPP TS 29.520.

This research implementation includes both Release 18-aligned NWDAF service
behavior and project-defined extensions for distributed and hierarchical
federated learning. The hierarchical topology fields and orchestration behavior
are experimental additions; they are not defined by 3GPP TS 29.520. The Go
process owns the external SBI and delegates model-training logic to its private
PyMTLF backend.

Requires Go 1.26.2. Linting requires golangci-lint v2.11.4.

## Build

```bash
make build
```

## Run

```bash
make run CONFIG=/path/to/nwdafcfg.yaml
```

## Test

```bash
make test
```

## Lint

```bash
make lint
```

## License

Apache-2.0
