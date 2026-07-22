package consumer

import (
	"context"
	"net/http"
	"time"

	"golang.org/x/oauth2"

	"github.com/free5gc/openapi"
)

const SmfEventExposurePath = "/nsmf-event-exposure/v1/subscriptions"

// NsmfService owns shared transport used by the standard SMF proxy.
type NsmfService struct {
	httpClient *http.Client
}

func NewNsmfService() *NsmfService {
	return &NsmfService{httpClient: &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns: 100, MaxIdleConnsPerHost: 10, IdleConnTimeout: 90 * time.Second,
		},
	}}
}

func BuildUpfEventSubs(upfNotifURI string, volume, throughput bool) []ExtendedEventSubscription {
	measurements := []MeasurementType{}
	if volume {
		measurements = append(measurements, MeasurementType_VOLUME_MEASUREMENT)
	}
	if throughput {
		measurements = append(measurements, MeasurementType_THROUGHPUT_MEASUREMENT)
	}
	return []ExtendedEventSubscription{{
		Event: SmfEvent_UPF_EVENT,
		UpfEvents: []UpfEvent{{
			Type: UpfEventType_USER_DATA_USAGE_MEASURES, MeasurementTypes: measurements,
			GranularityOfMeasurement: Granularity_PER_SESSION,
		}},
		BundlingAllowed: true, BundledEventNotifyUri: upfNotifURI,
	}}
}

func bindOAuthTokenToRequest(request *http.Request, requestCtx context.Context) error {
	if requestCtx == nil {
		return nil
	}
	tokenSource, ok := requestCtx.Value(openapi.ContextOAuth2).(oauth2.TokenSource)
	if !ok {
		return nil
	}
	token, err := tokenSource.Token()
	if err != nil {
		return err
	}
	token.SetAuthHeader(request)
	return nil
}

func (s *NsmfService) HTTPClient() *http.Client { return s.httpClient }
