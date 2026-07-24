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
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"golang.org/x/net/http2"
	"golang.org/x/oauth2"
	"golang.org/x/sync/singleflight"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
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
	nfDiscoveryClients  map[string]*NFDiscovery.APIClient
	accessTokenClients  map[string]*AccessToken.APIClient
	httpClientFactory   func(string) (*http.Client, error)
	initialRetryDelay   time.Duration
	maximumRetryDelay   time.Duration
	now                 func() time.Time
	discoveryCache      map[string]cachedDiscovery
	discoveryGroup      singleflight.Group
}

type NFDiscoveryQuery struct {
	TargetNFType    models.NrfNfManagementNfType
	RequesterNFType models.NrfNfManagementNfType
	ServiceNames    []models.ServiceName
}

// NFDiscoveryResult keeps the generated model for Go callers while retaining
// the complete peer JSON envelope for backend pass-through and cache hits.
type NFDiscoveryResult struct {
	models.SearchResult
	raw map[string]json.RawMessage
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
		nfDiscoveryClients:  make(map[string]*NFDiscovery.APIClient),
		accessTokenClients:  make(map[string]*AccessToken.APIClient),
		httpClientFactory:   newNRFHTTPClient,
		initialRetryDelay:   initialNRFRegistrationRetry,
		maximumRetryDelay:   maximumNRFRegistrationRetry,
		now:                 time.Now,
		discoveryCache:      make(map[string]cachedDiscovery),
	}
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
	query NFDiscoveryQuery,
) (*NFDiscoveryResult, error) {
	if nwdafCtx == nil {
		return nil, errors.New("NWDAF context is unavailable")
	}
	client, err := s.getNFDiscoveryClient(nwdafCtx.NrfUri())
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
		request := &NFDiscovery.SearchNFInstancesRequest{}
		request.SetTargetNfType(query.TargetNFType)
		request.SetRequesterNfType(query.RequesterNFType)
		request.SetRequesterNfInstanceId(nwdafCtx.NfId)
		request.SetServiceNames(query.ServiceNames)

		capture := &rawDiscoveryCapture{}
		capturedContext := context.WithValue(
			requestCtx,
			rawDiscoveryCaptureContextKey{},
			capture,
		)
		response, searchErr := client.NFInstancesStoreApi.SearchNFInstances(capturedContext, request)
		if searchErr != nil {
			if ctx.Err() != nil {
				return nil, fmt.Errorf("NRF SMF discovery canceled: %w", ctx.Err())
			}
			var apiErr openapi.GenericOpenAPIError
			if errors.As(searchErr, &apiErr) {
				standardError := &NFDiscoveryError{StatusCode: apiErr.ErrorStatus}
				switch model := apiErr.Model().(type) {
				case NFDiscovery.SearchNFInstancesError:
					standardError.Location = model.Location
					standardError.ProblemDetails = model.ProblemDetails
				case *NFDiscovery.SearchNFInstancesError:
					if model != nil {
						standardError.Location = model.Location
						standardError.ProblemDetails = model.ProblemDetails
					}
				case models.ProblemDetails:
					standardError.ProblemDetails = model
				case *models.ProblemDetails:
					if model != nil {
						standardError.ProblemDetails = *model
					}
				}
				return nil, standardError
			}
			return nil, fmt.Errorf("NRF SMF discovery failed: %w", searchErr)
		}
		if response == nil {
			return nil, errors.New("malformed NRF SMF discovery success: response is nil")
		}
		result, parseErr := parseNFDiscoveryResult(capture.body, response.SearchResult)
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
	typed models.SearchResult,
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
	if typed.NfInstances == nil {
		typed.NfInstances = []models.NrfNfDiscoveryNfProfile{}
	}
	typed.ValidityPeriod = validity
	return NFDiscoveryResult{SearchResult: typed, raw: envelope}, nil
}

func discoveryCacheKey(query NFDiscoveryQuery, nrfURI, requesterNFInstanceID string) string {
	names := make([]string, len(query.ServiceNames))
	for index, name := range query.ServiceNames {
		names[index] = string(name)
	}
	sort.Strings(names)
	return nrfURI + "|" + requesterNFInstanceID + "|" +
		string(query.TargetNFType) + "|" + string(query.RequesterNFType) + "|" +
		strings.Join(names, ",")
}

func (s *NrfService) DiscoverSmfProfiles(
	ctx context.Context,
	nwdafCtx *nwdaf_context.NWDAFContext,
) (*NFDiscoveryResult, error) {
	return s.DiscoverNFInstances(ctx, nwdafCtx, NFDiscoveryQuery{
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

	profile := nwdafCtx.NFProfile()
	if profile.NfInstanceId == "" {
		return RegistrationResult{}, errors.New("register NF instance requires configured NF profile")
	}
	client, err := s.getNFManagementClient(nwdafCtx.NrfUri())
	if err != nil {
		return RegistrationResult{}, err
	}
	request := &NFManagement.RegisterNFInstanceRequest{
		NfInstanceID:             &profile.NfInstanceId,
		NrfNfManagementNfProfile: &profile,
	}

	startedAt := time.Now()
	for attempt := 1; ; attempt++ {
		response, registerErr := client.NFInstanceIDDocumentApi.RegisterNFInstance(ctx, request)
		if registerErr == nil {
			result, validateErr := validateRegistrationResponse(response, profile.NfInstanceId)
			if validateErr != nil {
				return result, validateErr
			}
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
			result := RegistrationResult{
				RemoteRegistered: isPotentialRegistrationResponseError(registerErr),
			}
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

func validateRegistrationResponse(
	response *NFManagement.RegisterNFInstanceResponse,
	expectedInstanceID string,
) (RegistrationResult, error) {
	result := RegistrationResult{RemoteRegistered: true}
	if response == nil {
		return result, errors.New("malformed NRF registration success: response is nil")
	}
	profile := response.NrfNfManagementNfProfile
	result.ResourceURI = response.Location
	result.HeartBeatTimer = profile.HeartBeatTimer
	if profile.CustomInfo != nil {
		value, ok := profile.CustomInfo["oauth2"]
		if ok {
			oauth2Required, valid := value.(bool)
			if !valid {
				return result, errors.New("malformed NRF registration success: customInfo.oauth2 is not boolean")
			}
			result.OAuth2Required = oauth2Required
		}
	}
	if profile.NfInstanceId == "" {
		return result, errors.New("malformed NRF registration success: nfInstanceId is missing")
	}
	if profile.NfInstanceId != expectedInstanceID {
		return result, fmt.Errorf(
			"malformed NRF registration success: nfInstanceId %q does not match %q",
			profile.NfInstanceId,
			expectedInstanceID,
		)
	}
	if profile.NfType != models.NrfNfManagementNfType_NWDAF {
		return result, fmt.Errorf(
			"malformed NRF registration success: nfType is %q",
			profile.NfType,
		)
	}
	if profile.NfStatus != models.NrfNfManagementNfStatus_REGISTERED {
		return result, fmt.Errorf(
			"malformed NRF registration success: nfStatus is %q",
			profile.NfStatus,
		)
	}

	if response.Location != "" {
		location, err := url.Parse(response.Location)
		if err != nil || path.Base(location.Path) != expectedInstanceID {
			return result, fmt.Errorf(
				"malformed NRF registration success: Location does not identify NF instance %q",
				expectedInstanceID,
			)
		}
	}

	return result, nil
}

func isPotentialRegistrationResponseError(err error) bool {
	var apiErr openapi.GenericOpenAPIError
	if errors.As(err, &apiErr) {
		return false
	}
	var urlError *url.Error
	if errors.As(err, &urlError) {
		return false
	}
	var networkError net.Error
	return !errors.As(err, &networkError)
}

func isRetryableRegistrationError(err error) bool {
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
