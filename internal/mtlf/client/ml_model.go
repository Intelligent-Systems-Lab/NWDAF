package client

import (
	"context"
	"net/http"
	"net/url"

	"github.com/free5gc/nwdaf/internal/backend"
	wire "github.com/free5gc/nwdaf/internal/compat/mlmodel"
	trainingwire "github.com/free5gc/nwdaf/internal/compat/mlmodeltraining"
)

const (
	mlModelProvisionSubscriptionsPath   = "/internal/v1/ml-model-provision/subscriptions"
	mlModelMonitorRegistrationsPath     = "/internal/v1/ml-model-monitor/registrations"
	mlModelMonitorNotificationsPath     = "/internal/v1/ml-model-monitor/notifications"
	mlModelTrainingSubscriptionsPath    = "/internal/v1/ml-model-training/subscriptions"
	mlModelTrainingNotificationsPath    = "/internal/v1/ml-model-training/notifications"
	mlModelTrainingSubscriptionIDHeader = "X-NWDAF-Subscription-Id"
)

func (c *BackendClient) CreateMLModelProvisionSubscription(
	ctx context.Context,
	body []byte,
) (*backend.StandardResponse, error) {
	return c.doMLModelRequest(
		ctx,
		http.MethodPost,
		c.endpoint+mlModelProvisionSubscriptionsPath,
		body,
		"create ML Model Provision subscription",
		backend.StandardOperationContract{
			SuccessValidators: map[int]func([]byte) error{
				http.StatusCreated: validateProvisionSubscription,
			},
			ErrorStatuses: createErrorStatuses(),
		},
	)
}

func (c *BackendClient) CreateMLModelTrainingSubscription(
	ctx context.Context,
	body []byte,
	subscriptionID string,
) (*backend.StandardResponse, error) {
	return c.doMLModelRequest(
		ctx, http.MethodPost, c.endpoint+mlModelTrainingSubscriptionsPath, body,
		"create ML Model Training subscription",
		backend.StandardOperationContract{
			RequestHeaders: http.Header{mlModelTrainingSubscriptionIDHeader: []string{subscriptionID}},
			SuccessValidators: map[int]func([]byte) error{
				http.StatusCreated: validateTrainingSubscription,
			},
			ErrorStatuses: createErrorStatuses(),
		},
	)
}

func (c *BackendClient) ReplaceMLModelTrainingSubscription(
	ctx context.Context,
	subscriptionID string,
	body []byte,
) (*backend.StandardResponse, error) {
	return c.doMLModelRequest(
		ctx, http.MethodPut,
		c.endpoint+mlModelTrainingSubscriptionsPath+"/"+url.PathEscape(subscriptionID),
		body, "replace ML Model Training subscription",
		backend.StandardOperationContract{
			SuccessValidators: map[int]func([]byte) error{
				http.StatusOK: validateTrainingSubscription, http.StatusNoContent: nil,
			},
			ErrorStatuses: createErrorStatuses(),
		},
	)
}

func (c *BackendClient) PatchMLModelTrainingSubscription(
	ctx context.Context,
	subscriptionID string,
	body []byte,
) (*backend.StandardResponse, error) {
	return c.doMLModelRequest(
		ctx, http.MethodPatch,
		c.endpoint+mlModelTrainingSubscriptionsPath+"/"+url.PathEscape(subscriptionID),
		body, "patch ML Model Training subscription",
		backend.StandardOperationContract{
			SuccessValidators: map[int]func([]byte) error{
				http.StatusOK: validateTrainingSubscription, http.StatusNoContent: nil,
			},
			ErrorStatuses:      createErrorStatuses(),
			RequestContentType: "application/merge-patch+json",
		},
	)
}

func (c *BackendClient) DeleteMLModelTrainingSubscription(
	ctx context.Context,
	subscriptionID string,
) (*backend.StandardResponse, error) {
	return c.doMLModelRequest(
		ctx, http.MethodDelete,
		c.endpoint+mlModelTrainingSubscriptionsPath+"/"+url.PathEscape(subscriptionID),
		nil, "delete ML Model Training subscription",
		backend.StandardOperationContract{
			SuccessValidators: map[int]func([]byte) error{http.StatusNoContent: nil},
			ErrorStatuses:     deleteErrorStatuses(),
		},
	)
}

func (c *BackendClient) DeliverMLModelTrainingNotification(
	ctx context.Context,
	body []byte,
) (*backend.StandardResponse, error) {
	return c.doMLModelRequest(
		ctx, http.MethodPost, c.endpoint+mlModelTrainingNotificationsPath, body,
		"deliver ML Model Training notification",
		backend.StandardOperationContract{
			SuccessValidators: map[int]func([]byte) error{http.StatusNoContent: nil},
			ErrorStatuses:     createErrorStatuses(),
		},
	)
}

func (c *BackendClient) ReplaceMLModelProvisionSubscription(
	ctx context.Context,
	subscriptionID string,
	body []byte,
) (*backend.StandardResponse, error) {
	return c.doMLModelRequest(
		ctx,
		http.MethodPut,
		c.endpoint+mlModelProvisionSubscriptionsPath+"/"+url.PathEscape(subscriptionID),
		body,
		"replace ML Model Provision subscription",
		backend.StandardOperationContract{
			SuccessValidators: map[int]func([]byte) error{
				http.StatusOK:        validateProvisionSubscription,
				http.StatusNoContent: nil,
			},
			ErrorStatuses: createErrorStatuses(),
		},
	)
}

func (c *BackendClient) DeleteMLModelProvisionSubscription(
	ctx context.Context,
	subscriptionID string,
) (*backend.StandardResponse, error) {
	return c.doMLModelRequest(
		ctx,
		http.MethodDelete,
		c.endpoint+mlModelProvisionSubscriptionsPath+"/"+url.PathEscape(subscriptionID),
		nil,
		"delete ML Model Provision subscription",
		backend.StandardOperationContract{
			SuccessValidators: map[int]func([]byte) error{http.StatusNoContent: nil},
			ErrorStatuses:     deleteErrorStatuses(),
		},
	)
}

func (c *BackendClient) CreateMLModelMonitorRegistration(
	ctx context.Context,
	body []byte,
) (*backend.StandardResponse, error) {
	return c.doMLModelRequest(
		ctx,
		http.MethodPost,
		c.endpoint+mlModelMonitorRegistrationsPath,
		body,
		"create ML Model Monitor registration",
		backend.StandardOperationContract{
			SuccessValidators: map[int]func([]byte) error{
				http.StatusCreated: validateMonitorRegistration,
			},
			ErrorStatuses: createErrorStatuses(),
		},
	)
}

func (c *BackendClient) DeleteMLModelMonitorRegistration(
	ctx context.Context,
	registrationID string,
) (*backend.StandardResponse, error) {
	return c.doMLModelRequest(
		ctx,
		http.MethodDelete,
		c.endpoint+mlModelMonitorRegistrationsPath+"/"+url.PathEscape(registrationID),
		nil,
		"delete ML Model Monitor registration",
		backend.StandardOperationContract{
			SuccessValidators: map[int]func([]byte) error{http.StatusNoContent: nil},
			ErrorStatuses:     deleteErrorStatuses(),
		},
	)
}

func (c *BackendClient) DeliverMLModelMonitorNotification(
	ctx context.Context,
	body []byte,
) (*backend.StandardResponse, error) {
	return c.doMLModelRequest(
		ctx,
		http.MethodPost,
		c.endpoint+mlModelMonitorNotificationsPath,
		body,
		"deliver ML Model Monitor notification",
		backend.StandardOperationContract{
			SuccessValidators: map[int]func([]byte) error{http.StatusNoContent: nil},
			ErrorStatuses:     createErrorStatuses(),
		},
	)
}

func (c *BackendClient) doMLModelRequest(
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

func deleteErrorStatuses() map[int]struct{} {
	return backend.ErrorStatuses(
		http.StatusBadRequest,
		http.StatusUnauthorized,
		http.StatusForbidden,
		http.StatusNotFound,
		http.StatusTooManyRequests,
		http.StatusInternalServerError,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
	)
}

func validateProvisionSubscription(body []byte) error {
	_, err := wire.ParseMLModelProvisionSubscription(body)
	return err
}

func validateMonitorRegistration(body []byte) error {
	_, err := wire.ParseMLModelMonitorRegistration(body)
	return err
}

func validateTrainingSubscription(body []byte) error {
	value, err := trainingwire.ParseNwdafMLModelTrainSubsc(body)
	if err != nil {
		return err
	}
	return trainingwire.ValidateFLSubscription(value, nil)
}
