# NWDAF

Network Data Analytics Function (NWDAF) implementation based on 3GPP TS 29.520.

## Prerequisites

- Go 1.26.2
- golangci-lint v2.11.4 for `make lint`

Current runtime notes:

- The main SBI server supports both HTTP and HTTPS, selected through runtime
  configuration.
- NRF registration is enabled by default. In that mode `nrfUri` is required:
  NWDAF registers its Events Subscription service before starting owned
  listeners, retries temporary NRF transport/server failures until
  cancellation, and attempts bounded deregistration before SBI shutdown.
  Configured-endpoint deployments may set `nrfRegistrationEnabled: false`;
  this permits an empty `nrfUri`, skips registration/heartbeat/deregistration,
  and makes private NRF discovery requests fail with `503`.
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
- The private `GET /internal/v1/nrf/nf-instances` boundary accepts the
  standard NF Discovery query property names used by the current AnLF and MTLF
  backends. It supports NWDAF, SMF, UDM, and ADRF targets within the implemented
  query matrix. Go injects the containing NWDAF requester identity, forwards
  the request to NRF, preserves Release 18 profiles in the `SearchResult`, and
  shares valid results by a canonical query key. Backend processes still own
  candidate selection. Unsupported or malformed filter combinations fail
  before an NRF request is sent.
- The AnLF and MTLF backends independently select ADRF origins. Their standard-shaped
  storage and retrieval-control requests carry `Target-Api-Root`; Go validates
  that origin, performs ADRF POST/DELETE as the containing NWDAF, and forwards
  complete ADRF retrieval callbacks to the MTLF backend before acknowledging
  them. Dataset record GETs are performed directly by the MTLF backend.
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

## Role-Aware Configuration

`configuration.nfInstanceId` may pin the UUIDv4 used for NRF registration so
that one deployment identity survives process restarts.
`configuration.serviceNameList` is the explicit list of public services that
the process mounts and registers; `configuration.nwdafInfo` is the Release 18
capability description and uses the standard JSON property names in YAML:

```yaml
configuration:
  nfInstanceId: "11111111-1111-4111-8111-111111111111"
  serviceNameList:
    - nnwdaf-eventssubscription
    - nnwdaf-mlmodelmonitor
  nwdafInfo:
    nwdafEvents: [UE_COMMUNICATION]
    mlAnalyticsList:
      - mlAnalyticsIds: [UE_COMMUNICATION]
        trackingAreaList:
          - plmnId: {mcc: "466", mnc: "92"}
            tac: "000001"
        mlModelInterInfo:
          vendorList: ["001122"]
        flCapabilityType: FL_CLIENT
        nfTypeList: [UPF]
```

The runtime rejects advertised services without the required backend and
capability entry. Existing configs that omit both explicit profile fields keep
the legacy backend-derived service behavior. The distributed role examples are
`config/nwdafcfg-a.yaml`, `config/nwdafcfg-b.yaml`, and
`config/nwdafcfg-c.yaml`. They do not advertise Model Training because that
public service is not implemented yet.

## Testing

```bash
# Unit tests
go test ./...

# Lint
make lint
```

## License

Apache-2.0
