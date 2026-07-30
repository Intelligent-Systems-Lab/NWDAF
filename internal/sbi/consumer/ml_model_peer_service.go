package consumer

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/free5gc/nwdaf/internal/backend"
	wire "github.com/free5gc/nwdaf/internal/compat/mlmodel"
	trainingwire "github.com/free5gc/nwdaf/internal/compat/mlmodeltraining"
)

const mlModelPeerTimeout = 30 * time.Second

func (c *Consumer) CreatePeerMLModelProvision(
	ctx context.Context,
	target backend.SelectedTarget,
	body []byte,
) (*backend.StandardResponse, error) {
	if err := c.validateSelectedTarget(target); err != nil {
		return nil, &backend.ContractError{
			Operation: "create peer ML Model Provision subscription",
			Detail:    err.Error(),
		}
	}
	return c.executePeerMLModelRequest(
		ctx,
		http.MethodPost,
		peerCollectionURI(target, "nnwdaf-mlmodelprovision", "subscriptions"),
		body,
		"create peer ML Model Provision subscription",
		map[int]func([]byte) error{http.StatusCreated: validateProvisionRepresentation},
	)
}

func (c *Consumer) CreatePeerMLModelTraining(
	ctx context.Context,
	target backend.SelectedTarget,
	body []byte,
) (*backend.StandardResponse, error) {
	if err := c.validateSelectedTarget(target); err != nil {
		return nil, &backend.ContractError{
			Operation: "create peer ML Model Training subscription", Detail: err.Error(),
		}
	}
	return c.executePeerMLModelRequest(
		ctx, http.MethodPost,
		peerCollectionURI(target, "nnwdaf-mlmodeltraining", "subscriptions"),
		body, "create peer ML Model Training subscription",
		map[int]func([]byte) error{http.StatusCreated: validateTrainingRepresentation},
	)
}

func (c *Consumer) ReplacePeerMLModelTraining(
	ctx context.Context,
	location string,
	body []byte,
) (*backend.StandardResponse, error) {
	return c.executePeerMLModelRequest(
		ctx, http.MethodPut, location, body, "replace peer ML Model Training subscription",
		map[int]func([]byte) error{
			http.StatusOK: validateTrainingRepresentation, http.StatusNoContent: nil,
		},
	)
}

func (c *Consumer) PatchPeerMLModelTraining(
	ctx context.Context,
	location string,
	body []byte,
) (*backend.StandardResponse, error) {
	return c.executePeerMLModelRequestWithContentType(
		ctx, http.MethodPatch, location, body, "patch peer ML Model Training subscription",
		map[int]func([]byte) error{
			http.StatusOK: validateTrainingRepresentation, http.StatusNoContent: nil,
		},
		"application/merge-patch+json",
	)
}

func (c *Consumer) DeletePeerMLModelTraining(
	ctx context.Context,
	location string,
) (*backend.StandardResponse, error) {
	return c.executePeerMLModelRequest(
		ctx, http.MethodDelete, location, nil, "delete peer ML Model Training subscription",
		map[int]func([]byte) error{http.StatusNoContent: nil},
	)
}

func (c *Consumer) ReplacePeerMLModelProvision(
	ctx context.Context,
	location string,
	body []byte,
) (*backend.StandardResponse, error) {
	return c.executePeerMLModelRequest(
		ctx,
		http.MethodPut,
		location,
		body,
		"replace peer ML Model Provision subscription",
		map[int]func([]byte) error{
			http.StatusOK:        validateProvisionRepresentation,
			http.StatusNoContent: nil,
		},
	)
}

func (c *Consumer) DeletePeerMLModelProvision(
	ctx context.Context,
	location string,
) (*backend.StandardResponse, error) {
	return c.executePeerMLModelRequest(
		ctx,
		http.MethodDelete,
		location,
		nil,
		"delete peer ML Model Provision subscription",
		map[int]func([]byte) error{http.StatusNoContent: nil},
	)
}

func (c *Consumer) CreatePeerMLModelMonitorRegistration(
	ctx context.Context,
	target backend.SelectedTarget,
	body []byte,
) (*backend.StandardResponse, error) {
	if err := c.validateSelectedTarget(target); err != nil {
		return nil, &backend.ContractError{
			Operation: "create peer ML Model Monitor registration",
			Detail:    err.Error(),
		}
	}
	return c.executePeerMLModelRequest(
		ctx,
		http.MethodPost,
		peerCollectionURI(target, "nnwdaf-mlmodelmonitor", "registrations"),
		body,
		"create peer ML Model Monitor registration",
		map[int]func([]byte) error{http.StatusCreated: validateMonitorRegistration},
	)
}

func (c *Consumer) DeletePeerMLModelMonitorRegistration(
	ctx context.Context,
	location string,
) (*backend.StandardResponse, error) {
	return c.executePeerMLModelRequest(
		ctx,
		http.MethodDelete,
		location,
		nil,
		"delete peer ML Model Monitor registration",
		map[int]func([]byte) error{http.StatusNoContent: nil},
	)
}

func (c *Consumer) CreatePeerMLModelMonitorSubscription(
	ctx context.Context,
	target backend.SelectedTarget,
	body []byte,
) (*backend.StandardResponse, error) {
	if err := c.validateSelectedTarget(target); err != nil {
		return nil, &backend.ContractError{
			Operation: "create peer ML Model Monitor subscription",
			Detail:    err.Error(),
		}
	}
	return c.executePeerMLModelRequest(
		ctx,
		http.MethodPost,
		peerCollectionURI(target, "nnwdaf-mlmodelmonitor", "subscriptions"),
		body,
		"create peer ML Model Monitor subscription",
		map[int]func([]byte) error{http.StatusCreated: validateMonitorSubscription},
	)
}

func (c *Consumer) validateSelectedTarget(target backend.SelectedTarget) error {
	if c == nil {
		return errors.New("NWDAF consumer is unavailable")
	}
	if target.SelectionSource == backend.SelectionSourceConfigured {
		return nil
	}
	return c.nrfService.ValidateCachedSelectedTarget(target)
}

func (c *Consumer) ReplacePeerMLModelMonitorSubscription(
	ctx context.Context,
	location string,
	body []byte,
) (*backend.StandardResponse, error) {
	return c.executePeerMLModelRequest(
		ctx,
		http.MethodPut,
		location,
		body,
		"replace peer ML Model Monitor subscription",
		map[int]func([]byte) error{
			http.StatusOK:        validateMonitorSubscription,
			http.StatusNoContent: nil,
		},
	)
}

func (c *Consumer) DeletePeerMLModelMonitorSubscription(
	ctx context.Context,
	location string,
) (*backend.StandardResponse, error) {
	return c.executePeerMLModelRequest(
		ctx,
		http.MethodDelete,
		location,
		nil,
		"delete peer ML Model Monitor subscription",
		map[int]func([]byte) error{http.StatusNoContent: nil},
	)
}

func (c *Consumer) executePeerMLModelRequest(
	ctx context.Context,
	method,
	requestURI string,
	body []byte,
	operation string,
	success map[int]func([]byte) error,
) (*backend.StandardResponse, error) {
	return c.executePeerMLModelRequestWithContentType(
		ctx, method, requestURI, body, operation, success, "",
	)
}

func (c *Consumer) executePeerMLModelRequestWithContentType(
	ctx context.Context,
	method,
	requestURI string,
	body []byte,
	operation string,
	success map[int]func([]byte) error,
	requestContentType string,
) (*backend.StandardResponse, error) {
	if strings.TrimSpace(requestURI) == "" {
		return nil, &backend.ContractError{Operation: operation, Detail: "peer URI is required"}
	}
	client := c.mlModelPeerHTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	return backend.ExecuteStandardRequest(
		ctx,
		client,
		mlModelPeerTimeout,
		method,
		requestURI,
		body,
		operation,
		backend.StandardOperationContract{
			SuccessValidators: success,
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
			FollowRedirects:    true,
			RequestContentType: requestContentType,
		},
	)
}

func validateTrainingRepresentation(body []byte) error {
	value, err := trainingwire.ParseNwdafMLModelTrainSubsc(body)
	if err != nil {
		return err
	}
	return trainingwire.ValidateFLSubscription(value, nil)
}

func peerCollectionURI(
	target backend.SelectedTarget,
	expectedService,
	collection string,
) string {
	if target.ServiceName != expectedService {
		return ""
	}
	base := strings.TrimRight(target.APIRoot, "/")
	servicePrefix := "/" + expectedService + "/v1"
	if !strings.HasSuffix(base, servicePrefix) {
		base += servicePrefix
	}
	endpoint, err := url.JoinPath(base, collection)
	if err != nil {
		return ""
	}
	return endpoint
}

func validateProvisionRepresentation(body []byte) error {
	_, err := wire.ParseMLModelProvisionSubscription(body)
	return err
}

func validateMonitorRegistration(body []byte) error {
	_, err := wire.ParseMLModelMonitorRegistration(body)
	return err
}

func validateMonitorSubscription(body []byte) error {
	_, err := wire.ParseMLModelMonitorSubscription(body)
	return err
}
