package consumer

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"golang.org/x/net/http2"
	"golang.org/x/oauth2"
	"golang.org/x/sync/singleflight"

	"github.com/free5gc/nwdaf/internal/backend"
	compatnrf "github.com/free5gc/nwdaf/internal/compat/nrf"
	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/openapi"
	"github.com/free5gc/openapi/models"
	"github.com/free5gc/openapi/nrf/AccessToken"
	"github.com/free5gc/openapi/nrf/NFDiscovery"
	"github.com/free5gc/openapi/nrf/NFManagement"
	sbi_metrics "github.com/free5gc/util/metrics/sbi"
)

const (
	initialNRFRegistrationRetry = 2 * time.Second
	maximumNRFRegistrationRetry = 30 * time.Second
	httpsScheme                 = "https"
)

var ErrUnsupportedNRFRedirect = errors.New("NRF redirect is unsupported")

type RegistrationResult struct {
	ResourceURI      string
	OAuth2Required   bool
	RemoteRegistered bool
	HeartBeatTimer   int32
}

type NFManagementService interface {
	RegisterNFInstance(ctx context.Context) (RegistrationResult, error)
	DeregisterNFInstance(ctx context.Context) error
}

type NrfService struct {
	mu                  sync.Mutex
	nfManagementClients map[string]*NFManagement.APIClient
	nfManagementHTTP    map[string]*http.Client
	nfDiscoveryClients  map[string]*NFDiscovery.APIClient
	accessTokenClients  map[string]*AccessToken.APIClient
	httpClientFactory   func(string) (*http.Client, error)
	initialRetryDelay   time.Duration
	maximumRetryDelay   time.Duration
	now                 func() time.Time
	discoveryCache      map[string]cachedDiscovery
	discoveryGroup      singleflight.Group
}

// NFDiscoveryResult keeps the generated model for Go callers while retaining
// the complete peer JSON envelope for backend pass-through and cache hits.
type NFDiscoveryResult struct {
	models.SearchResult
	raw      map[string]json.RawMessage
	profiles []compatnrf.NFProfile
}

func (r NFDiscoveryResult) CompatibilityProfiles() []compatnrf.NFProfile {
	profiles := make([]compatnrf.NFProfile, len(r.profiles))
	copy(profiles, r.profiles)
	return profiles
}

func (r NFDiscoveryResult) MarshalJSON() ([]byte, error) {
	if r.raw == nil {
		return json.Marshal(r.SearchResult)
	}
	envelope := make(map[string]json.RawMessage, len(r.raw))
	for key, value := range r.raw {
		envelope[key] = value
	}
	validity, err := json.Marshal(r.ValidityPeriod)
	if err != nil {
		return nil, err
	}
	envelope["validityPeriod"] = validity
	return json.Marshal(envelope)
}

type cachedDiscovery struct {
	result    NFDiscoveryResult
	expiresAt time.Time
	usedAt    time.Time
}

type rawDiscoveryCapture struct {
	body []byte
}

type rawDiscoveryCaptureContextKey struct{}

type rawDiscoveryCaptureTransport struct {
	base http.RoundTripper
}

func (t rawDiscoveryCaptureTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := t.base.RoundTrip(request)
	if err != nil || response == nil {
		return response, err
	}
	capture, ok := request.Context().Value(rawDiscoveryCaptureContextKey{}).(*rawDiscoveryCapture)
	if !ok || response.StatusCode != http.StatusOK {
		return response, nil
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	closeErr := response.Body.Close()
	if closeErr != nil {
		return nil, closeErr
	}
	capture.body = append(capture.body[:0], body...)
	response.Body = io.NopCloser(bytes.NewReader(body))
	return response, nil
}

func newNrfService() *NrfService {
	return &NrfService{
		nfManagementClients: make(map[string]*NFManagement.APIClient),
		nfManagementHTTP:    make(map[string]*http.Client),
		nfDiscoveryClients:  make(map[string]*NFDiscovery.APIClient),
		accessTokenClients:  make(map[string]*AccessToken.APIClient),
		httpClientFactory:   newNRFHTTPClient,
		initialRetryDelay:   initialNRFRegistrationRetry,
		maximumRetryDelay:   maximumNRFRegistrationRetry,
		now:                 time.Now,
		discoveryCache:      make(map[string]cachedDiscovery),
	}
}

func (s *NrfService) getNFManagementHTTPClient(nrfURI string) (*http.Client, error) {
	if nrfURI == "" {
		return nil, errors.New("NRF URI is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if client, ok := s.nfManagementHTTP[nrfURI]; ok {
		return client, nil
	}
	httpClientFactory := s.httpClientFactory
	if httpClientFactory == nil {
		httpClientFactory = newNRFHTTPClient
	}
	client, err := httpClientFactory(nrfURI)
	if err != nil {
		return nil, err
	}
	s.nfManagementHTTP[nrfURI] = client
	return client, nil
}

func (s *NrfService) getNFDiscoveryClient(nrfURI string) (*NFDiscovery.APIClient, error) {
	if nrfURI == "" {
		return nil, errors.New("NRF URI is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if client, ok := s.nfDiscoveryClients[nrfURI]; ok {
		return client, nil
	}

	configuration := NFDiscovery.NewConfiguration()
	configuration.SetBasePath(nrfURI)
	configuration.SetMetrics(sbi_metrics.SbiMetricHook)
	httpClientFactory := s.httpClientFactory
	if httpClientFactory == nil {
		httpClientFactory = newNRFHTTPClient
	}
	httpClient, err := httpClientFactory(nrfURI)
	if err != nil {
		return nil, err
	}
	transport := httpClient.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	httpClient.Transport = rawDiscoveryCaptureTransport{base: transport}
	configuration.SetHTTPClient(httpClient)
	client := NFDiscovery.NewAPIClient(configuration)
	s.nfDiscoveryClients[nrfURI] = client
	return client, nil
}

func (s *NrfService) getAccessTokenClient(nrfURI string) (*AccessToken.APIClient, error) {
	if nrfURI == "" {
		return nil, errors.New("NRF URI is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if client, ok := s.accessTokenClients[nrfURI]; ok {
		return client, nil
	}

	configuration := AccessToken.NewConfiguration()
	configuration.SetBasePath(nrfURI)
	configuration.SetMetrics(sbi_metrics.SbiMetricHook)
	httpClientFactory := s.httpClientFactory
	if httpClientFactory == nil {
		httpClientFactory = newNRFHTTPClient
	}
	httpClient, err := httpClientFactory(nrfURI)
	if err != nil {
		return nil, err
	}
	configuration.SetHTTPClient(httpClient)
	client := AccessToken.NewAPIClient(configuration)
	s.accessTokenClients[nrfURI] = client
	return client, nil
}

func (s *NrfService) getNFManagementClient(nrfURI string) (*NFManagement.APIClient, error) {
	if nrfURI == "" {
		return nil, errors.New("NRF URI is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if client, ok := s.nfManagementClients[nrfURI]; ok {
		return client, nil
	}

	configuration := NFManagement.NewConfiguration()
	configuration.SetBasePath(nrfURI)
	configuration.SetMetrics(sbi_metrics.SbiMetricHook)
	httpClientFactory := s.httpClientFactory
	if httpClientFactory == nil {
		httpClientFactory = newNRFHTTPClient
	}
	httpClient, err := httpClientFactory(nrfURI)
	if err != nil {
		return nil, err
	}
	configuration.SetHTTPClient(httpClient)
	client := NFManagement.NewAPIClient(configuration)
	s.nfManagementClients[nrfURI] = client
	return client, nil
}

func newNRFHTTPClient(nrfURI string) (*http.Client, error) {
	parsed, err := url.Parse(nrfURI)
	if err != nil {
		return nil, fmt.Errorf("parse NRF URI for HTTP client: %w", err)
	}

	transport := &http2.Transport{
		ReadIdleTimeout: openapi.ReadIdleTimeoutPeriod,
		PingTimeout:     openapi.PingTimeoutPeriod,
	}
	switch parsed.Scheme {
	case "http":
		transport.AllowHTTP = true
		transport.DialTLSContext = func(
			ctx context.Context,
			network string,
			addr string,
			_ *tls.Config,
		) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, addr)
		}
	case httpsScheme:
		transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	default:
		return nil, fmt.Errorf("unsupported NRF URI scheme %q", parsed.Scheme)
	}

	return &http.Client{
		Transport: otelhttp.NewTransport(transport),
		Timeout:   openapi.TimeoutPeriod,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}, nil
}

type NFDiscoveryError struct {
	StatusCode     int
	Location       string
	ProblemDetails models.ProblemDetails
}

func (e *NFDiscoveryError) Error() string {
	return fmt.Sprintf("NRF NF discovery failed: status=%d", e.StatusCode)
}

func (e *NFDiscoveryError) HTTPStatusCode() int {
	return e.StatusCode
}

func (e *NFDiscoveryError) StandardProblemDetails() *models.ProblemDetails {
	problem := e.ProblemDetails
	if problem.Status == 0 {
		problem.Status = int32(e.StatusCode)
	}
	return &problem
}

func (e *NFDiscoveryError) RedirectLocation() string {
	return e.Location
}

// DiscoverSmfProfiles performs the standard SMF NFDiscovery query and returns
// the complete SearchResult. Candidate selection and validity caching belong to
// the AnLF backend on this boundary.
func (s *NrfService) DiscoverNFInstances(
	ctx context.Context,
	nwdafCtx *nwdaf_context.NWDAFContext,
	query backend.NFDiscoveryQuery,
) (*NFDiscoveryResult, error) {
	if nwdafCtx == nil {
		return nil, errors.New("NWDAF context is unavailable")
	}
	client, err := s.getNFManagementHTTPClient(nwdafCtx.NrfUri())
	if err != nil {
		return nil, fmt.Errorf("create NRF NFDiscovery client: %w", err)
	}
	requestCtx := ctx
	if nwdafCtx.RegistrationState().OAuth2Required {
		requestCtx, err = s.getTokenContext(
			ctx,
			nwdafCtx,
			models.ServiceName_NNRF_DISC,
			models.NrfNfManagementNfType_NRF,
		)
		if err != nil {
			return nil, fmt.Errorf("authorize NRF SMF discovery: %w", err)
		}
	}
	key := discoveryCacheKey(query, nwdafCtx.NrfUri(), nwdafCtx.NfId)
	now := s.now()
	s.mu.Lock()
	if cached, ok := s.discoveryCache[key]; ok && now.Before(cached.expiresAt) {
		cached.usedAt = now
		cached.result.ValidityPeriod = int32(cached.expiresAt.Sub(now) / time.Second)
		if cached.result.ValidityPeriod > 0 {
			s.discoveryCache[key] = cached
			result := cached.result
			s.mu.Unlock()
			return &result, nil
		}
	}
	delete(s.discoveryCache, key)
	s.mu.Unlock()

	value, err, _ := s.discoveryGroup.Do(key, func() (any, error) {
		endpoint, buildErr := buildNFDiscoveryURL(
			nwdafCtx.NrfUri(),
			nwdafCtx.NfId,
			query,
		)
		if buildErr != nil {
			return nil, buildErr
		}
		request, requestErr := http.NewRequestWithContext(
			requestCtx,
			http.MethodGet,
			endpoint,
			nil,
		)
		if requestErr != nil {
			return nil, fmt.Errorf("create NRF discovery request: %w", requestErr)
		}
		request.Header.Set("Accept", "application/json, application/problem+json")
		if source, ok := requestCtx.Value(openapi.ContextOAuth2).(oauth2.TokenSource); ok {
			token, tokenErr := source.Token()
			if tokenErr != nil {
				return nil, fmt.Errorf("obtain NRF discovery access token: %w", tokenErr)
			}
			token.SetAuthHeader(request)
		}
		response, searchErr := client.Do(request)
		if searchErr != nil {
			if ctx.Err() != nil {
				return nil, fmt.Errorf("NRF discovery canceled: %w", ctx.Err())
			}
			return nil, fmt.Errorf("NRF discovery failed: %w", searchErr)
		}
		defer func() {
			if closeErr := response.Body.Close(); closeErr != nil {
				logger.ConsLog.Warnf("Close NRF discovery response body: %v", closeErr)
			}
		}()
		body, readErr := io.ReadAll(io.LimitReader(response.Body, 8<<20))
		if readErr != nil {
			return nil, fmt.Errorf("read NRF discovery response: %w", readErr)
		}
		if response.StatusCode != http.StatusOK {
			standardError := &NFDiscoveryError{
				StatusCode: response.StatusCode,
				Location:   response.Header.Get("Location"),
			}
			if len(bytes.TrimSpace(body)) > 0 {
				if decodeErr := json.Unmarshal(body, &standardError.ProblemDetails); decodeErr != nil {
					standardError.ProblemDetails.Detail = "NRF returned a malformed problem response"
				}
			}
			return nil, standardError
		}
		result, parseErr := parseNFDiscoveryResult(body)
		if parseErr != nil {
			return nil, fmt.Errorf("malformed NRF discovery success: %w", parseErr)
		}
		if result.ValidityPeriod > 0 {
			cacheNow := s.now()
			s.mu.Lock()
			for cacheKey, entry := range s.discoveryCache {
				if !cacheNow.Before(entry.expiresAt) {
					delete(s.discoveryCache, cacheKey)
				}
			}
			if len(s.discoveryCache) >= 256 {
				oldestKey := ""
				var oldest time.Time
				for cacheKey, entry := range s.discoveryCache {
					if oldestKey == "" || entry.usedAt.Before(oldest) {
						oldestKey, oldest = cacheKey, entry.usedAt
					}
				}
				delete(s.discoveryCache, oldestKey)
			}
			s.discoveryCache[key] = cachedDiscovery{
				result:    result,
				expiresAt: cacheNow.Add(time.Duration(result.ValidityPeriod) * time.Second),
				usedAt:    cacheNow,
			}
			s.mu.Unlock()
		}
		return result, nil
	})
	if err != nil {
		return nil, err
	}
	result := value.(NFDiscoveryResult)
	return &result, nil
}

func parseNFDiscoveryResult(
	raw []byte,
) (NFDiscoveryResult, error) {
	var envelope map[string]json.RawMessage
	if len(raw) == 0 {
		return NFDiscoveryResult{}, errors.New("raw response body is unavailable")
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return NFDiscoveryResult{}, fmt.Errorf("invalid JSON: %w", err)
	}
	rawValidity, ok := envelope["validityPeriod"]
	if !ok {
		return NFDiscoveryResult{}, errors.New("validityPeriod is missing")
	}
	var validity int32
	if err := json.Unmarshal(rawValidity, &validity); err != nil || validity < 0 {
		return NFDiscoveryResult{}, errors.New("validityPeriod is invalid")
	}
	rawInstances, ok := envelope["nfInstances"]
	if !ok || bytes.Equal(bytes.TrimSpace(rawInstances), []byte("null")) {
		return NFDiscoveryResult{}, errors.New("nfInstances is missing")
	}
	var instances []json.RawMessage
	if err := json.Unmarshal(rawInstances, &instances); err != nil {
		return NFDiscoveryResult{}, errors.New("nfInstances is invalid")
	}
	var typed models.SearchResult
	if err := json.Unmarshal(raw, &typed); err != nil {
		return NFDiscoveryResult{}, fmt.Errorf("generated SearchResult decode failed: %w", err)
	}
	profiles := make([]compatnrf.NFProfile, 0, len(instances))
	for index, instance := range instances {
		var profile compatnrf.NFProfile
		if err := json.Unmarshal(instance, &profile); err != nil {
			return NFDiscoveryResult{}, fmt.Errorf("nfInstances[%d] is invalid: %w", index, err)
		}
		profiles = append(profiles, profile)
	}
	if typed.NfInstances == nil {
		typed.NfInstances = []models.NrfNfDiscoveryNfProfile{}
	}
	typed.ValidityPeriod = validity
	return NFDiscoveryResult{SearchResult: typed, raw: envelope, profiles: profiles}, nil
}

func discoveryCacheKey(
	query backend.NFDiscoveryQuery,
	nrfURI,
	requesterNFInstanceID string,
) string {
	values, err := serializeNFDiscoveryQuery(requesterNFInstanceID, query)
	if err != nil {
		return "invalid|" + err.Error()
	}
	return strings.TrimRight(nrfURI, "/") + "|" + values.Encode()
}

func buildNFDiscoveryURL(
	nrfURI string,
	requesterNFInstanceID string,
	query backend.NFDiscoveryQuery,
) (string, error) {
	endpoint, err := url.JoinPath(nrfURI, "nnrf-disc", "v1", "nf-instances")
	if err != nil {
		return "", fmt.Errorf("build NRF discovery endpoint: %w", err)
	}
	values, err := serializeNFDiscoveryQuery(requesterNFInstanceID, query)
	if err != nil {
		return "", err
	}
	return endpoint + "?" + values.Encode(), nil
}

func serializeNFDiscoveryQuery(
	requesterNFInstanceID string,
	query backend.NFDiscoveryQuery,
) (url.Values, error) {
	values := url.Values{}
	values.Set("target-nf-type", string(query.TargetNFType))
	values.Set("requester-nf-type", string(query.RequesterNFType))
	values.Set("requester-nf-instance-id", requesterNFInstanceID)
	if query.TargetNFInstanceID != "" {
		values.Set("target-nf-instance-id", query.TargetNFInstanceID)
	}
	if len(query.ServiceNames) > 0 {
		names := make([]string, 0, len(query.ServiceNames))
		for _, name := range query.ServiceNames {
			names = append(names, string(name))
		}
		values.Set("service-names", strings.Join(sortedUniqueStrings(names), ","))
	}
	if len(query.NwdafEventList) > 0 {
		events := make([]string, 0, len(query.NwdafEventList))
		for _, event := range query.NwdafEventList {
			events = append(events, string(event))
		}
		values.Set("nwdaf-event-list", strings.Join(sortedUniqueStrings(events), ","))
	}
	if query.MLAnalyticsInfoList != nil {
		canonical, err := canonicalMLAnalyticsInfoList(query.MLAnalyticsInfoList)
		if err != nil {
			return nil, fmt.Errorf("encode ml-analytics-info-list: %w", err)
		}
		values.Set("ml-analytics-info-list", string(canonical))
	}
	if query.InternalGroupIdentity != "" {
		values.Set("internal-group-identity", query.InternalGroupIdentity)
	}
	if query.MLModelStorageInd != nil {
		values.Set("ml-model-storage-ind", strconv.FormatBool(*query.MLModelStorageInd))
	}
	if query.DataStorageInd != nil {
		values.Set("data-storage-ind", strconv.FormatBool(*query.DataStorageInd))
	}
	return values, nil
}

func canonicalMLAnalyticsInfoList(
	input []compatnrf.MLAnalyticsInfo,
) ([]byte, error) {
	if len(input) == 0 {
		return nil, errors.New("list must contain at least one entry")
	}
	entries := make([]json.RawMessage, 0, len(input))
	for _, value := range input {
		value.MLAnalyticsIDs = sortedUniqueNwdafEvents(value.MLAnalyticsIDs)
		value.SNSSAIList = sortedUniqueJSONValues(value.SNSSAIList)
		value.TrackingAreaList = sortedUniqueJSONValues(value.TrackingAreaList)
		value.NFTypeList = sortedUniqueNFTypes(value.NFTypeList)
		value.NFSetIDList = sortedUniqueStrings(value.NFSetIDList)
		if value.MLModelInteroperabilityInfo != nil {
			value.MLModelInteroperabilityInfo.VendorList = sortedUniqueStrings(
				value.MLModelInteroperabilityInfo.VendorList,
			)
		}
		encoded, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		entries = append(entries, encoded)
	}
	sort.Slice(entries, func(left, right int) bool {
		return bytes.Compare(entries[left], entries[right]) < 0
	})
	return json.Marshal(entries)
}

func sortedUniqueNwdafEvents(values []models.NwdafEvent) []models.NwdafEvent {
	stringsValue := make([]string, 0, len(values))
	for _, value := range values {
		stringsValue = append(stringsValue, string(value))
	}
	normalized := sortedUniqueStrings(stringsValue)
	result := make([]models.NwdafEvent, 0, len(normalized))
	for _, value := range normalized {
		result = append(result, models.NwdafEvent(value))
	}
	return result
}

func sortedUniqueNFTypes(
	values []models.NrfNfManagementNfType,
) []models.NrfNfManagementNfType {
	stringsValue := make([]string, 0, len(values))
	for _, value := range values {
		stringsValue = append(stringsValue, string(value))
	}
	normalized := sortedUniqueStrings(stringsValue)
	result := make([]models.NrfNfManagementNfType, 0, len(normalized))
	for _, value := range normalized {
		result = append(result, models.NrfNfManagementNfType(value))
	}
	return result
}

func sortedUniqueJSONValues[T any](values []T) []T {
	byJSON := make(map[string]T, len(values))
	keys := make([]string, 0, len(values))
	for _, value := range values {
		encoded, err := json.Marshal(value)
		if err != nil {
			continue
		}
		key := string(encoded)
		if _, exists := byJSON[key]; exists {
			continue
		}
		byJSON[key] = value
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]T, 0, len(keys))
	for _, key := range keys {
		result = append(result, byJSON[key])
	}
	return result
}

func sortedUniqueStrings(values []string) []string {
	result := append([]string(nil), values...)
	sort.Strings(result)
	if len(result) == 0 {
		return result
	}
	write := 1
	for read := 1; read < len(result); read++ {
		if result[read] == result[write-1] {
			continue
		}
		result[write] = result[read]
		write++
	}
	return result[:write]
}

func (s *NrfService) DiscoverSmfProfiles(
	ctx context.Context,
	nwdafCtx *nwdaf_context.NWDAFContext,
) (*NFDiscoveryResult, error) {
	return s.DiscoverNFInstances(ctx, nwdafCtx, backend.NFDiscoveryQuery{
		TargetNFType:    models.NrfNfManagementNfType_SMF,
		RequesterNFType: models.NrfNfManagementNfType_NWDAF,
		ServiceNames:    []models.ServiceName{models.ServiceName_NSMF_EVENT_EXPOSURE},
	})
}

func (s *NrfService) RegisterNFInstance(
	ctx context.Context,
	nwdafCtx *nwdaf_context.NWDAFContext,
) (RegistrationResult, error) {
	if ctx == nil {
		return RegistrationResult{}, errors.New("register NF instance requires context")
	}
	if nwdafCtx == nil {
		return RegistrationResult{}, errors.New("register NF instance requires NWDAF context")
	}

	profile, err := nwdafCtx.NFProfileSnapshot()
	if err != nil {
		return RegistrationResult{}, err
	}
	if profile.NfInstanceId == "" {
		return RegistrationResult{}, errors.New("register NF instance requires configured NF profile")
	}
	client, err := s.getNFManagementHTTPClient(nwdafCtx.NrfUri())
	if err != nil {
		return RegistrationResult{}, err
	}
	endpoint, err := url.JoinPath(
		nwdafCtx.NrfUri(),
		"nnrf-nfm",
		"v1",
		"nf-instances",
		profile.NfInstanceId,
	)
	if err != nil {
		return RegistrationResult{}, fmt.Errorf("build NRF registration URI: %w", err)
	}
	body, err := json.Marshal(profile)
	if err != nil {
		return RegistrationResult{}, fmt.Errorf("encode Release 18 NF profile: %w", err)
	}

	startedAt := time.Now()
	for attempt := 1; ; attempt++ {
		result, registerErr := registerNFProfile(ctx, client, endpoint, body, profile.NfInstanceId)
		if registerErr == nil {
			if attempt > 1 {
				consumerLog.Infof(
					"NRF registration recovered after %d attempts in %s",
					attempt,
					time.Since(startedAt).Round(time.Millisecond),
				)
			}
			return result, nil
		}

		if ctx.Err() != nil {
			return RegistrationResult{}, fmt.Errorf("register NF instance canceled: %w", ctx.Err())
		}
		if !isRetryableRegistrationError(registerErr) {
			return result, classifyRegistrationError(registerErr)
		}

		delay := s.retryDelay(attempt)
		if attempt == 1 {
			consumerLog.Errorf(
				"NRF registration failed; retrying: nrfUri=%s cause=%v nextDelay=%s",
				nwdafCtx.NrfUri(),
				registerErr,
				delay,
			)
		} else if shouldLogRegistrationRetry(attempt) {
			consumerLog.Warnf(
				"NRF registration still unavailable: attempt=%d nextDelay=%s cause=%v",
				attempt,
				delay,
				registerErr,
			)
		}
		if waitErr := waitForRegistrationRetry(ctx, delay); waitErr != nil {
			return RegistrationResult{}, waitErr
		}
	}
}

type registrationHTTPError struct {
	statusCode     int
	location       string
	problemDetails models.ProblemDetails
}

func (e *registrationHTTPError) Error() string {
	return fmt.Sprintf("NRF registration HTTP status=%d", e.statusCode)
}

func registerNFProfile(
	ctx context.Context,
	client *http.Client,
	endpoint string,
	body []byte,
	expectedInstanceID string,
) (RegistrationResult, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, endpoint, bytes.NewReader(body))
	if err != nil {
		return RegistrationResult{}, fmt.Errorf("create NRF registration request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json, application/problem+json")
	response, err := client.Do(request)
	if err != nil {
		return RegistrationResult{}, err
	}
	defer func() {
		if closeErr := response.Body.Close(); closeErr != nil {
			logger.ConsLog.Warnf("Close NRF registration response body: %v", closeErr)
		}
	}()
	responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, 4<<20))
	if readErr != nil {
		return RegistrationResult{}, fmt.Errorf("read NRF registration response: %w", readErr)
	}

	switch response.StatusCode {
	case http.StatusOK, http.StatusCreated:
		result := RegistrationResult{
			RemoteRegistered: true,
			ResourceURI:      response.Header.Get("Location"),
		}
		if response.StatusCode == http.StatusCreated && result.ResourceURI == "" {
			return result, errors.New("malformed NRF registration success: Location is required for 201")
		}
		var profile compatnrf.NFProfile
		if len(bytes.TrimSpace(responseBody)) == 0 {
			return result, errors.New("malformed NRF registration success: response body is empty")
		}
		if err = json.Unmarshal(responseBody, &profile); err != nil {
			return result, fmt.Errorf("malformed NRF registration success: %w", err)
		}
		result.HeartBeatTimer = profile.HeartBeatTimer
		if profile.CustomInfo != nil {
			if value, ok := profile.CustomInfo["oauth2"]; ok {
				oauth2Required, valid := value.(bool)
				if !valid {
					return result, errors.New(
						"malformed NRF registration success: customInfo.oauth2 is not boolean",
					)
				}
				result.OAuth2Required = oauth2Required
			}
		}
		if err = validateRegistrationProfile(&profile, expectedInstanceID, result.ResourceURI); err != nil {
			return result, err
		}
		return result, nil
	default:
		httpErr := &registrationHTTPError{
			statusCode: response.StatusCode,
			location:   response.Header.Get("Location"),
		}
		if len(bytes.TrimSpace(responseBody)) > 0 {
			if decodeErr := json.Unmarshal(responseBody, &httpErr.problemDetails); decodeErr != nil {
				httpErr.problemDetails.Detail = "NRF returned a malformed problem response"
			}
		}
		return RegistrationResult{}, httpErr
	}
}

func validateRegistrationProfile(
	profile *compatnrf.NFProfile,
	expectedInstanceID string,
	locationValue string,
) error {
	if profile == nil {
		return errors.New("malformed NRF registration success: profile is missing")
	}
	if profile.NfInstanceId == "" {
		return errors.New("malformed NRF registration success: nfInstanceId is missing")
	}
	if profile.NfInstanceId != expectedInstanceID {
		return fmt.Errorf(
			"malformed NRF registration success: nfInstanceId %q does not match %q",
			profile.NfInstanceId,
			expectedInstanceID,
		)
	}
	if profile.NfType != models.NrfNfManagementNfType_NWDAF {
		return fmt.Errorf("malformed NRF registration success: nfType is %q", profile.NfType)
	}
	if profile.NfStatus != models.NrfNfManagementNfStatus_REGISTERED {
		return fmt.Errorf("malformed NRF registration success: nfStatus is %q", profile.NfStatus)
	}
	if locationValue != "" {
		location, err := url.Parse(locationValue)
		if err != nil || path.Base(location.Path) != expectedInstanceID {
			return fmt.Errorf(
				"malformed NRF registration success: Location does not identify NF instance %q",
				expectedInstanceID,
			)
		}
	}
	return nil
}

func (s *NrfService) DeregisterNFInstance(
	ctx context.Context,
	nwdafCtx *nwdaf_context.NWDAFContext,
) error {
	if ctx == nil {
		return errors.New("deregister NF instance requires context")
	}
	if nwdafCtx == nil {
		return errors.New("deregister NF instance requires NWDAF context")
	}
	profile := nwdafCtx.NFProfile()
	if profile.NfInstanceId == "" {
		return errors.New("deregister NF instance requires configured NF profile")
	}
	client, err := s.getNFManagementClient(nwdafCtx.NrfUri())
	if err != nil {
		return err
	}
	requestCtx := ctx
	if nwdafCtx.RegistrationState().OAuth2Required {
		requestCtx, err = s.getTokenContext(
			ctx,
			nwdafCtx,
			models.ServiceName_NNRF_NFM,
			models.NrfNfManagementNfType_NRF,
		)
		if err != nil {
			return fmt.Errorf("authorize NRF deregistration: %w", err)
		}
	}

	request := &NFManagement.DeregisterNFInstanceRequest{NfInstanceID: &profile.NfInstanceId}
	response, deregisterErr := client.NFInstanceIDDocumentApi.DeregisterNFInstance(requestCtx, request)
	if deregisterErr == nil {
		if response == nil {
			return errors.New("malformed NRF deregistration success: response is nil")
		}
		return nil
	}
	if ctx.Err() != nil {
		return fmt.Errorf("deregister NF instance canceled: %w", ctx.Err())
	}

	var apiErr openapi.GenericOpenAPIError
	if errors.As(deregisterErr, &apiErr) {
		switch apiErr.ErrorStatus {
		case http.StatusNotFound:
			return nil
		case http.StatusTemporaryRedirect, http.StatusPermanentRedirect:
			return fmt.Errorf("%w: status=%d", ErrUnsupportedNRFRedirect, apiErr.ErrorStatus)
		default:
			return fmt.Errorf("deregister NF instance failed: status=%d", apiErr.ErrorStatus)
		}
	}
	return fmt.Errorf("deregister NF instance failed: %w", deregisterErr)
}

func (s *NrfService) getTokenContext(
	ctx context.Context,
	nwdafCtx *nwdaf_context.NWDAFContext,
	serviceName models.ServiceName,
	targetNF models.NrfNfManagementNfType,
) (context.Context, error) {
	if ctx == nil {
		return nil, errors.New("access token request requires context")
	}
	if nwdafCtx == nil {
		return nil, errors.New("access token request requires NWDAF context")
	}
	client, err := s.getAccessTokenClient(nwdafCtx.NrfUri())
	if err != nil {
		return nil, err
	}

	request := &AccessToken.AccessTokenRequestRequest{}
	request.SetGrantType("client_credentials")
	request.SetNfInstanceId(nwdafCtx.NfId)
	request.SetNfType(models.NrfNfManagementNfType_NWDAF)
	request.SetTargetNfType(targetNF)
	request.SetScope(string(serviceName))

	response, requestErr := client.AccessTokenRequestApi.AccessTokenRequest(ctx, request)
	if requestErr != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("access token request canceled: %w", ctx.Err())
		}
		var apiErr openapi.GenericOpenAPIError
		if errors.As(requestErr, &apiErr) {
			switch apiErr.ErrorStatus {
			case http.StatusTemporaryRedirect, http.StatusPermanentRedirect:
				return nil, fmt.Errorf("%w: status=%d", ErrUnsupportedNRFRedirect, apiErr.ErrorStatus)
			default:
				return nil, fmt.Errorf("access token request failed: status=%d", apiErr.ErrorStatus)
			}
		}
		return nil, fmt.Errorf("access token request failed: %w", requestErr)
	}
	if response == nil || response.NrfAccessTokenAccessTokenRsp.AccessToken == "" {
		return nil, errors.New("malformed access token response: access_token is empty")
	}

	token := &oauth2.Token{
		AccessToken: response.NrfAccessTokenAccessTokenRsp.AccessToken,
		TokenType:   response.NrfAccessTokenAccessTokenRsp.TokenType,
	}
	return context.WithValue(ctx, openapi.ContextOAuth2, oauth2.StaticTokenSource(token)), nil
}

func isRetryableRegistrationError(err error) bool {
	var httpErr *registrationHTTPError
	if errors.As(err, &httpErr) {
		return httpErr.statusCode >= http.StatusInternalServerError &&
			httpErr.statusCode <= http.StatusNetworkAuthenticationRequired
	}
	var apiErr openapi.GenericOpenAPIError
	if errors.As(err, &apiErr) {
		return apiErr.ErrorStatus >= http.StatusInternalServerError &&
			apiErr.ErrorStatus <= http.StatusNetworkAuthenticationRequired
	}

	var unknownAuthority x509.UnknownAuthorityError
	if errors.As(err, &unknownAuthority) {
		return false
	}
	var hostnameError x509.HostnameError
	if errors.As(err, &hostnameError) {
		return false
	}
	var certificateError x509.CertificateInvalidError
	if errors.As(err, &certificateError) {
		return false
	}
	var urlError *url.Error
	if errors.As(err, &urlError) {
		return true
	}
	var networkError net.Error
	return errors.As(err, &networkError)
}

func classifyRegistrationError(err error) error {
	var httpErr *registrationHTTPError
	if errors.As(err, &httpErr) {
		switch httpErr.statusCode {
		case http.StatusTemporaryRedirect, http.StatusPermanentRedirect:
			return fmt.Errorf("%w: status=%d", ErrUnsupportedNRFRedirect, httpErr.statusCode)
		default:
			if httpErr.statusCode >= http.StatusOK && httpErr.statusCode < http.StatusMultipleChoices {
				return fmt.Errorf(
					"unexpected NRF registration success: status=%d; contract supports only 200 and 201",
					httpErr.statusCode,
				)
			}
			return fmt.Errorf("register NF instance rejected by NRF: status=%d", httpErr.statusCode)
		}
	}
	var apiErr openapi.GenericOpenAPIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorStatus {
		case http.StatusTemporaryRedirect, http.StatusPermanentRedirect:
			return fmt.Errorf("%w: status=%d", ErrUnsupportedNRFRedirect, apiErr.ErrorStatus)
		default:
			if apiErr.ErrorStatus >= http.StatusOK && apiErr.ErrorStatus < http.StatusMultipleChoices {
				return fmt.Errorf(
					"unexpected NRF registration success: status=%d; generated contract supports only 200 and 201",
					apiErr.ErrorStatus,
				)
			}
			return fmt.Errorf("register NF instance rejected by NRF: status=%d", apiErr.ErrorStatus)
		}
	}
	return fmt.Errorf("register NF instance failed: %w", err)
}

func (s *NrfService) retryDelay(attempt int) time.Duration {
	delay := s.initialRetryDelay
	if delay <= 0 {
		delay = initialNRFRegistrationRetry
	}
	maximum := s.maximumRetryDelay
	if maximum < delay {
		maximum = delay
	}
	for retry := 1; retry < attempt && delay < maximum; retry++ {
		if delay > maximum/2 {
			return maximum
		}
		delay *= 2
	}
	if delay > maximum {
		return maximum
	}
	return delay
}

func waitForRegistrationRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return fmt.Errorf("register NF instance canceled: %w", ctx.Err())
	case <-timer.C:
		return nil
	}
}

func shouldLogRegistrationRetry(attempt int) bool {
	return attempt <= 3 || attempt&(attempt-1) == 0
}
