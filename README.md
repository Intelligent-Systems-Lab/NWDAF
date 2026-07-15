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
- When registration reports that OAuth is required, NWDAF obtains an
  `nnrf-nfm` access token before deregistration, an `nnrf-disc` token for NRF
  discovery, and an `nsmf-event-exposure` token for SMF subscription creation
  and deletion. It validates bearer tokens on the Events Subscription producer
  routes with the `nnwdaf-eventssubscription` scope. `nrfCertPem` identifies
  the NRF public certificate used for token verification; it is separate from
  the NWDAF SBI TLS certificate and private key. Missing or unusable
  verification material is logged without stopping startup, but protected
  inbound requests fail authorization until it is supplied.
- SMF data collection requires an explicit `smf.endpointSource`. Use `nrf` to
  discover all registered SMFs that expose `nsmf-event-exposure`, or
  `configured` with `smf.endpoints` for isolated/fake-SMF environments. The
  modes never merge and NRF mode never silently falls back to configured
  endpoints. Discovery results are reused only for the positive NRF
  `validityPeriod`; refresh failures do not return stale endpoints.
- OAuth-enabled HTTP/H2C is the current free5GC integration target.
  OAuth-enabled HTTPS with NRF-required mutual TLS, redirect handling,
  UDM-backed serving-SMF resolution, and NFUpdate heartbeat remain later work.
- The main SBI server currently mounts free5GC's inbound request metrics
  middleware, and the NRF NFManagement, NFDiscovery, and Access Token generated
  clients install the outbound SBI metrics hook. Collectors, hooks for the
  remaining outbound clients, and the separately owned `/metrics` server are
  not wired yet; completing that free5GC metrics pattern remains an
  independent, default-disabled supporting workstream and does not block the
  NRF path.

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
