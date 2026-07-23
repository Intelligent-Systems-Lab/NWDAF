package client

import (
	"context"
	"net/http"
	"net/url"

	"github.com/free5gc/nwdaf/internal/backend"
	"github.com/free5gc/nwdaf/internal/mlmodel/wire"
)

const (
	mlModelProvisionNotificationPath = "/internal/v1/ml-model-provision/subscriptions"
	mlModelMonitorSubscriptionsPath  = "/internal/v1/ml-model-monitor/subscriptions"
)

func (c *Client) DeliverMLModelProvisionNotification(
	ctx context.Context,
	subscriptionID string,
	body []byte,
) (*backend.StandardResponse, error) {
	return c.doMLModelRequest(
		ctx,
		http.MethodPost,
		c.endpoint+mlModelProvisionNotificationPath+"/"+url.PathEscape(subscriptionID)+"/notifications",
		body,
		"deliver ML Model Provision notification",
		backend.StandardOperationContract{
			SuccessValidators: map[int]func([]byte) error{http.StatusNoContent: nil},
			ErrorStatuses:     createErrorStatuses(),
		},
	)
}

func (c *Client) CreateMLModelMonitorSubscription(
	ctx context.Context,
	body []byte,
) (*backend.StandardResponse, error) {
	return c.doMLModelRequest(
		ctx,
		http.MethodPost,
		c.endpoint+mlModelMonitorSubscriptionsPath,
		body,
		"create ML Model Monitor subscription",
		backend.StandardOperationContract{
			SuccessValidators: map[int]func([]byte) error{
				http.StatusCreated: validateMonitorSubscription,
			},
			ErrorStatuses: createErrorStatuses(),
		},
	)
}

func (c *Client) ReplaceMLModelMonitorSubscription(
	ctx context.Context,
	subscriptionID string,
	body []byte,
) (*backend.StandardResponse, error) {
	return c.doMLModelRequest(
		ctx,
		http.MethodPut,
		c.endpoint+mlModelMonitorSubscriptionsPath+"/"+url.PathEscape(subscriptionID),
		body,
		"replace ML Model Monitor subscription",
		backend.StandardOperationContract{
			SuccessValidators: map[int]func([]byte) error{
				http.StatusOK:        validateMonitorSubscription,
				http.StatusNoContent: nil,
			},
			ErrorStatuses: backend.ErrorStatuses(
				http.StatusBadRequest,
				http.StatusUnauthorized,
				http.StatusForbidden,
				http.StatusNotFound,
				http.StatusLengthRequired,
				http.StatusRequestEntityTooLarge,
				http.StatusUnsupportedMediaType,
				http.StatusTooManyRequests,
				http.StatusInternalServerError,
				http.StatusNotImplemented,
				http.StatusBadGateway,
				http.StatusServiceUnavailable,
			),
		},
	)
}

func (c *Client) DeleteMLModelMonitorSubscription(
	ctx context.Context,
	subscriptionID string,
) (*backend.StandardResponse, error) {
	return c.doMLModelRequest(
		ctx,
		http.MethodDelete,
		c.endpoint+mlModelMonitorSubscriptionsPath+"/"+url.PathEscape(subscriptionID),
		nil,
		"delete ML Model Monitor subscription",
		backend.StandardOperationContract{
			SuccessValidators: map[int]func([]byte) error{http.StatusNoContent: nil},
			ErrorStatuses: backend.ErrorStatuses(
				http.StatusBadRequest,
				http.StatusUnauthorized,
				http.StatusForbidden,
				http.StatusNotFound,
				http.StatusTooManyRequests,
				http.StatusInternalServerError,
				http.StatusNotImplemented,
				http.StatusBadGateway,
				http.StatusServiceUnavailable,
			),
		},
	)
}

func (c *Client) doMLModelRequest(
	ctx context.Context,
	method string,
	requestURL string,
	body []byte,
	operation string,
	contract backend.StandardOperationContract,
) (*backend.StandardResponse, error) {
	return backend.ExecuteStandardRequest(
		ctx,
		c.httpClient,
		c.timeout,
		method,
		requestURL,
		body,
		operation,
		contract,
	)
}

func createErrorStatuses() map[int]struct{} {
	return backend.ErrorStatuses(
		http.StatusBadRequest,
		http.StatusUnauthorized,
		http.StatusForbidden,
		http.StatusNotFound,
		http.StatusLengthRequired,
		http.StatusRequestEntityTooLarge,
		http.StatusUnsupportedMediaType,
		http.StatusTooManyRequests,
		http.StatusInternalServerError,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
	)
}

func validateMonitorSubscription(body []byte) error {
	_, err := wire.ParseMLModelMonitorSubscription(body)
	return err
}
