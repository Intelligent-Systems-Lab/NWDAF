package consumer

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"golang.org/x/net/http2"
	"golang.org/x/oauth2"

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

type discoveryQueryKey struct {
	nrfURI          string
	oauth2Required  bool
	targetNFType    models.NrfNfManagementNfType
	requesterNFType models.NrfNfManagementNfType
	serviceName     models.ServiceName
}

type discoveryCacheEntry struct {
	serviceRoots []string
	expiresAt    time.Time
}

type discoveryCall struct {
	done         chan struct{}
	serviceRoots []string
	err          error
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

	discoveryMu       sync.Mutex
	discoveryCache    map[discoveryQueryKey]discoveryCacheEntry
	discoveryInFlight map[discoveryQueryKey]*discoveryCall
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
		discoveryCache:      make(map[discoveryQueryKey]discoveryCacheEntry),
		discoveryInFlight:   make(map[discoveryQueryKey]*discoveryCall),
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
	case "https":
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

func (s *NrfService) DiscoverSmfEventExposure(
	ctx context.Context,
	nwdafCtx *nwdaf_context.NWDAFContext,
) ([]string, error) {
	if ctx == nil {
		return nil, errors.New("discover SMF Event Exposure requires context")
	}
	if nwdafCtx == nil {
		return nil, errors.New("discover SMF Event Exposure requires NWDAF context")
	}

	state := nwdafCtx.RegistrationState()
	key := discoveryQueryKey{
		nrfURI:          nwdafCtx.NrfUri(),
		oauth2Required:  state.OAuth2Required,
		targetNFType:    models.NrfNfManagementNfType_SMF,
		requesterNFType: models.NrfNfManagementNfType_NWDAF,
		serviceName:     models.ServiceName_NSMF_EVENT_EXPOSURE,
	}
	if key.nrfURI == "" {
		return nil, errors.New("discover SMF Event Exposure requires NRF URI")
	}

	now := time.Now
	if s.now != nil {
		now = s.now
	}
	s.discoveryMu.Lock()
	if cached, ok := s.discoveryCache[key]; ok {
		if now().Before(cached.expiresAt) {
			roots := append([]string(nil), cached.serviceRoots...)
			s.discoveryMu.Unlock()
			consumerLog.Debugf("NRF SMF discovery cache hit: service=%s endpoints=%d", key.serviceName, len(roots))
			return roots, nil
		}
		delete(s.discoveryCache, key)
		consumerLog.Debugf("NRF SMF discovery cache expired: service=%s", key.serviceName)
	}
	if inFlight, ok := s.discoveryInFlight[key]; ok {
		s.discoveryMu.Unlock()
		consumerLog.Debugf("NRF SMF discovery waiting for in-flight query: service=%s", key.serviceName)
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("discover SMF Event Exposure canceled while waiting: %w", ctx.Err())
		case <-inFlight.done:
			return append([]string(nil), inFlight.serviceRoots...), inFlight.err
		}
	}
	inFlight := &discoveryCall{done: make(chan struct{})}
	s.discoveryInFlight[key] = inFlight
	s.discoveryMu.Unlock()
	consumerLog.Debugf("NRF SMF discovery cache miss: service=%s", key.serviceName)

	var (
		serviceRoots   []string
		validityPeriod time.Duration
		discoveryErr   error
	)
	defer func() {
		if recovered := recover(); recovered != nil {
			discoveryErr = fmt.Errorf("discover SMF Event Exposure panicked: %v", recovered)
			s.completeDiscovery(key, inFlight, nil, 0, discoveryErr, now())
			panic(recovered)
		}
		s.completeDiscovery(key, inFlight, serviceRoots, validityPeriod, discoveryErr, now())
	}()

	serviceRoots, validityPeriod, discoveryErr = s.searchSmfEventExposure(ctx, nwdafCtx)
	return append([]string(nil), serviceRoots...), discoveryErr
}

func (s *NrfService) completeDiscovery(
	key discoveryQueryKey,
	inFlight *discoveryCall,
	serviceRoots []string,
	validityPeriod time.Duration,
	discoveryErr error,
	receivedAt time.Time,
) {
	s.discoveryMu.Lock()
	defer s.discoveryMu.Unlock()

	if discoveryErr == nil && validityPeriod > 0 {
		s.discoveryCache[key] = discoveryCacheEntry{
			serviceRoots: append([]string(nil), serviceRoots...),
			expiresAt:    receivedAt.Add(validityPeriod),
		}
	}
	inFlight.serviceRoots = append([]string(nil), serviceRoots...)
	inFlight.err = discoveryErr
	delete(s.discoveryInFlight, key)
	close(inFlight.done)
}

func (s *NrfService) searchSmfEventExposure(
	ctx context.Context,
	nwdafCtx *nwdaf_context.NWDAFContext,
) ([]string, time.Duration, error) {
	client, err := s.getNFDiscoveryClient(nwdafCtx.NrfUri())
	if err != nil {
		return nil, 0, fmt.Errorf("create NRF NFDiscovery client: %w", err)
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
			return nil, 0, fmt.Errorf("authorize NRF SMF discovery: %w", err)
		}
	}

	request := &NFDiscovery.SearchNFInstancesRequest{}
	request.SetTargetNfType(models.NrfNfManagementNfType_SMF)
	request.SetRequesterNfType(models.NrfNfManagementNfType_NWDAF)
	request.SetServiceNames([]models.ServiceName{models.ServiceName_NSMF_EVENT_EXPOSURE})

	response, searchErr := client.NFInstancesStoreApi.SearchNFInstances(requestCtx, request)
	if searchErr != nil {
		if ctx.Err() != nil {
			return nil, 0, fmt.Errorf("NRF SMF discovery canceled: %w", ctx.Err())
		}
		var apiErr openapi.GenericOpenAPIError
		if errors.As(searchErr, &apiErr) {
			switch apiErr.ErrorStatus {
			case http.StatusTemporaryRedirect, http.StatusPermanentRedirect:
				return nil, 0, fmt.Errorf("%w: status=%d", ErrUnsupportedNRFRedirect, apiErr.ErrorStatus)
			default:
				return nil, 0, fmt.Errorf("NRF SMF discovery failed: status=%d", apiErr.ErrorStatus)
			}
		}
		return nil, 0, fmt.Errorf("NRF SMF discovery failed: %w", searchErr)
	}
	if response == nil {
		return nil, 0, errors.New("malformed NRF SMF discovery success: response is nil")
	}

	serviceRoots := make([]string, 0, len(response.SearchResult.NfInstances))
	seen := make(map[string]struct{})
	for i := range response.SearchResult.NfInstances {
		root := openapi.GetNFServiceUri(
			&response.SearchResult.NfInstances[i],
			models.ServiceName_NSMF_EVENT_EXPOSURE,
		)
		root = strings.TrimRight(strings.TrimSpace(root), "/")
		if !usableServiceRoot(root) {
			continue
		}
		if _, ok := seen[root]; ok {
			continue
		}
		seen[root] = struct{}{}
		serviceRoots = append(serviceRoots, root)
	}
	if len(serviceRoots) == 0 {
		return nil, 0, errors.New("NRF SMF discovery returned no usable nsmf-event-exposure service roots")
	}

	consumerLog.Infof(
		"NRF SMF discovery succeeded: nrfUri=%s profiles=%d endpoints=%d service=%s",
		safeURIForLog(nwdafCtx.NrfUri()),
		len(response.SearchResult.NfInstances),
		len(serviceRoots),
		models.ServiceName_NSMF_EVENT_EXPOSURE,
	)
	validityPeriod := time.Duration(response.SearchResult.ValidityPeriod) * time.Second
	if response.SearchResult.ValidityPeriod <= 0 {
		validityPeriod = 0
	}
	return serviceRoots, validityPeriod, nil
}

func usableServiceRoot(raw string) bool {
	if raw == "" {
		return false
	}
	parsed, err := url.ParseRequestURI(raw)
	if err != nil || parsed.User != nil || parsed.Hostname() == "" ||
		parsed.RawQuery != "" || parsed.Fragment != "" {
		return false
	}
	return parsed.Scheme == "http" || parsed.Scheme == "https"
}

func safeURIForLog(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "<invalid>"
	}
	parsed.User = nil
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String()
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
