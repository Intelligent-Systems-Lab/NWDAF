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
	"sync"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"golang.org/x/net/http2"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/openapi"
	"github.com/free5gc/openapi/models"
	"github.com/free5gc/openapi/nrf/NFManagement"
	sbi_metrics "github.com/free5gc/util/metrics/sbi"
)

const (
	initialNRFRegistrationRetry = 2 * time.Second
	maximumNRFRegistrationRetry = 30 * time.Second
)

var (
	ErrOAuth2Required         = errors.New("NRF requires OAuth2; Phase 1 support is required")
	ErrUnsupportedNRFRedirect = errors.New("NRF redirect is unsupported in Phase 0")
)

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
	httpClientFactory   func(string) (*http.Client, error)
	initialRetryDelay   time.Duration
	maximumRetryDelay   time.Duration
}

func newNrfService() *NrfService {
	return &NrfService{
		nfManagementClients: make(map[string]*NFManagement.APIClient),
		httpClientFactory:   newNRFHTTPClient,
		initialRetryDelay:   initialNRFRegistrationRetry,
		maximumRetryDelay:   maximumNRFRegistrationRetry,
	}
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

	request := &NFManagement.DeregisterNFInstanceRequest{NfInstanceID: &profile.NfInstanceId}
	response, deregisterErr := client.NFInstanceIDDocumentApi.DeregisterNFInstance(ctx, request)
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
	if result.OAuth2Required {
		return result, ErrOAuth2Required
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
