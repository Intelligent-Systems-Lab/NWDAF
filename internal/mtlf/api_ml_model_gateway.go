package mtlf

import (
	"context"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/free5gc/nwdaf/internal/backend"
	wire "github.com/free5gc/nwdaf/internal/compat/mlmodel"
	trainingwire "github.com/free5gc/nwdaf/internal/compat/mlmodeltraining"
	"github.com/free5gc/nwdaf/internal/util"
	"github.com/free5gc/nwdaf/pkg/factory"
	"github.com/free5gc/openapi/models"
)

type mtlfMLModelGateway interface {
	HandleMLModelProvisionNotification(
		context.Context,
		string,
		[]byte,
	) (*backend.StandardResponse, *models.ProblemDetails)
	HandleCreateMLModelMonitorSubscriptionFromBackend(
		context.Context,
		[]byte,
		string,
		*backend.SelectedTarget,
	) (*backend.StandardResponse, *models.ProblemDetails)
	HandleReplaceMLModelMonitorSubscriptionFromBackend(
		context.Context,
		string,
		[]byte,
	) (*backend.StandardResponse, *models.ProblemDetails)
	HandleDeleteMLModelMonitorSubscriptionFromBackend(
		context.Context,
		string,
	) (*backend.StandardResponse, *models.ProblemDetails)
}

type mtlfMLModelTrainingGateway interface {
	HandleCreateMLModelTrainingFromBackend(
		context.Context,
		[]byte,
		*backend.SelectedTarget,
	) (*backend.StandardResponse, *models.ProblemDetails)
	HandleReplaceMLModelTrainingFromBackend(
		context.Context,
		string,
		[]byte,
	) (*backend.StandardResponse, *models.ProblemDetails)
	HandlePatchMLModelTrainingFromBackend(
		context.Context,
		string,
		[]byte,
	) (*backend.StandardResponse, *models.ProblemDetails)
	HandleDeleteMLModelTrainingFromBackend(
		context.Context,
		string,
	) (*backend.StandardResponse, *models.ProblemDetails)
	HandleMLModelTrainingNotification(
		context.Context,
		string,
		[]byte,
	) (*backend.StandardResponse, *models.ProblemDetails)
}

func (s *Server) mtlfMLModelRoutes() []Route {
	return []Route{
		{
			Name: "MLModelProvisionNotification", Method: http.MethodPost,
			Pattern: "/internal/v1/ml-model-provision/notifications",
			APIFunc: s.HandleMLModelProvisionNotification,
		},
		{
			Name: "CreateMLModelTrainingFromBackend", Method: http.MethodPost,
			Pattern: "/internal/v1/ml-model-training/subscriptions",
			APIFunc: s.HandleCreateMLModelTrainingFromBackend,
		},
		{
			Name: "ReplaceMLModelTrainingFromBackend", Method: http.MethodPut,
			Pattern: "/internal/v1/ml-model-training/subscriptions/:subscriptionId",
			APIFunc: s.HandleReplaceMLModelTrainingFromBackend,
		},
		{
			Name: "PatchMLModelTrainingFromBackend", Method: http.MethodPatch,
			Pattern: "/internal/v1/ml-model-training/subscriptions/:subscriptionId",
			APIFunc: s.HandlePatchMLModelTrainingFromBackend,
		},
		{
			Name: "DeleteMLModelTrainingFromBackend", Method: http.MethodDelete,
			Pattern: "/internal/v1/ml-model-training/subscriptions/:subscriptionId",
			APIFunc: s.HandleDeleteMLModelTrainingFromBackend,
		},
		{
			Name: "MLModelTrainingNotification", Method: http.MethodPost,
			Pattern: "/internal/v1/ml-model-training/notifications",
			APIFunc: s.HandleMLModelTrainingNotification,
		},
		{
			Name: "MLModelProvisionResourceNotification", Method: http.MethodPost,
			Pattern: "/internal/v1/ml-model-provision/subscriptions/:subscriptionId/notifications",
			APIFunc: s.HandleMLModelProvisionNotification,
		},
		{
			Name: "CreateMLModelMonitorSubscriptionFromBackend", Method: http.MethodPost,
			Pattern: "/internal/v1/ml-model-monitor/subscriptions",
			APIFunc: s.HandleCreateMLModelMonitorSubscriptionFromBackend,
		},
		{
			Name: "ReplaceMLModelMonitorSubscriptionFromBackend", Method: http.MethodPut,
			Pattern: "/internal/v1/ml-model-monitor/subscriptions/:subscriptionId",
			APIFunc: s.HandleReplaceMLModelMonitorSubscriptionFromBackend,
		},
		{
			Name: "DeleteMLModelMonitorSubscriptionFromBackend", Method: http.MethodDelete,
			Pattern: "/internal/v1/ml-model-monitor/subscriptions/:subscriptionId",
			APIFunc: s.HandleDeleteMLModelMonitorSubscriptionFromBackend,
		},
	}
}

func (s *Server) HandleCreateMLModelTrainingFromBackend(c *gin.Context) {
	body, ok := readMLModelGatewayBody(c, func(body []byte) error {
		_, err := trainingwire.ParseNwdafMLModelTrainSubsc(body)
		return err
	})
	if !ok {
		return
	}
	gateway, ok := s.processor.(mtlfMLModelTrainingGateway)
	if !ok {
		writeMLModelGatewayUnavailable(c)
		return
	}
	target, err := backend.ParseSelectedTargetHeaders(
		c.Request.Header, factory.NwdafMLModelTrainingServiceName,
	)
	if err != nil {
		util.GinProblemJson(c, malformedRequestProblem(err.Error()))
		return
	}
	response, problem := gateway.HandleCreateMLModelTrainingFromBackend(
		c.Request.Context(), body, target,
	)
	writeMLModelGatewayResponse(c, response, problem)
}

func (s *Server) HandleReplaceMLModelTrainingFromBackend(c *gin.Context) {
	body, ok := readMLModelGatewayBody(c, func(body []byte) error {
		_, err := trainingwire.ParseNwdafMLModelTrainSubsc(body)
		return err
	})
	if !ok {
		return
	}
	gateway, ok := s.processor.(mtlfMLModelTrainingGateway)
	if !ok {
		writeMLModelGatewayUnavailable(c)
		return
	}
	response, problem := gateway.HandleReplaceMLModelTrainingFromBackend(
		c.Request.Context(), c.Param("subscriptionId"), body,
	)
	writeMLModelGatewayResponse(c, response, problem)
}

func (s *Server) HandlePatchMLModelTrainingFromBackend(c *gin.Context) {
	body, ok := readMLModelGatewayBodyWithMediaType(
		c, "application/merge-patch+json", func(body []byte) error {
			_, err := trainingwire.ParseNwdafMLModelTrainSubscPatch(body)
			return err
		},
	)
	if !ok {
		return
	}
	gateway, ok := s.processor.(mtlfMLModelTrainingGateway)
	if !ok {
		writeMLModelGatewayUnavailable(c)
		return
	}
	response, problem := gateway.HandlePatchMLModelTrainingFromBackend(
		c.Request.Context(), c.Param("subscriptionId"), body,
	)
	writeMLModelGatewayResponse(c, response, problem)
}

func (s *Server) HandleDeleteMLModelTrainingFromBackend(c *gin.Context) {
	gateway, ok := s.processor.(mtlfMLModelTrainingGateway)
	if !ok {
		writeMLModelGatewayUnavailable(c)
		return
	}
	response, problem := gateway.HandleDeleteMLModelTrainingFromBackend(
		c.Request.Context(), c.Param("subscriptionId"),
	)
	writeMLModelGatewayResponse(c, response, problem)
}

func (s *Server) HandleMLModelTrainingNotification(c *gin.Context) {
	body, ok := readMLModelGatewayBody(c, func(body []byte) error {
		_, err := trainingwire.ParseNwdafMLModelTrainNotif(body)
		return err
	})
	if !ok {
		return
	}
	gateway, ok := s.processor.(mtlfMLModelTrainingGateway)
	if !ok {
		writeMLModelGatewayUnavailable(c)
		return
	}
	response, problem := gateway.HandleMLModelTrainingNotification(
		c.Request.Context(), "", body,
	)
	writeMLModelGatewayResponse(c, response, problem)
}

func (s *Server) HandleMLModelProvisionNotification(c *gin.Context) {
	body, ok := readMLModelGatewayBody(c, func(body []byte) error {
		_, err := wire.ParseMLModelProvisionNotifications(body)
		return err
	})
	if !ok {
		return
	}
	gateway, ok := s.processor.(mtlfMLModelGateway)
	if !ok {
		writeMLModelGatewayUnavailable(c)
		return
	}
	response, problem := gateway.HandleMLModelProvisionNotification(
		c.Request.Context(),
		c.Param("subscriptionId"),
		body,
	)
	writeMLModelGatewayResponse(c, response, problem)
}

func (s *Server) HandleCreateMLModelMonitorSubscriptionFromBackend(c *gin.Context) {
	body, ok := readMLModelGatewayBody(c, func(body []byte) error {
		_, err := wire.ParseMLModelMonitorSubscription(body)
		return err
	})
	if !ok {
		return
	}
	gateway, ok := s.processor.(mtlfMLModelGateway)
	if !ok {
		writeMLModelGatewayUnavailable(c)
		return
	}
	ownerRegistrationID := strings.TrimSpace(c.GetHeader(backend.MonitorRegistrationIDHeader))
	if ownerRegistrationID == "" {
		util.GinProblemJson(c, malformedRequestProblem(
			backend.MonitorRegistrationIDHeader+" header is required",
		))
		return
	}
	target, err := backend.ParseSelectedTargetHeaders(
		c.Request.Header,
		"nnwdaf-mlmodelmonitor",
	)
	if err != nil {
		util.GinProblemJson(c, malformedRequestProblem(err.Error()))
		return
	}
	response, problem := gateway.HandleCreateMLModelMonitorSubscriptionFromBackend(
		c.Request.Context(),
		body,
		ownerRegistrationID,
		target,
	)
	writeMLModelGatewayResponse(c, response, problem)
}

func (s *Server) HandleReplaceMLModelMonitorSubscriptionFromBackend(c *gin.Context) {
	body, ok := readMLModelGatewayBody(c, func(body []byte) error {
		_, err := wire.ParseMLModelMonitorSubscription(body)
		return err
	})
	if !ok {
		return
	}
	gateway, ok := s.processor.(mtlfMLModelGateway)
	if !ok {
		writeMLModelGatewayUnavailable(c)
		return
	}
	response, problem := gateway.HandleReplaceMLModelMonitorSubscriptionFromBackend(
		c.Request.Context(),
		c.Param("subscriptionId"),
		body,
	)
	writeMLModelGatewayResponse(c, response, problem)
}

func (s *Server) HandleDeleteMLModelMonitorSubscriptionFromBackend(c *gin.Context) {
	gateway, ok := s.processor.(mtlfMLModelGateway)
	if !ok {
		writeMLModelGatewayUnavailable(c)
		return
	}
	response, problem := gateway.HandleDeleteMLModelMonitorSubscriptionFromBackend(
		c.Request.Context(),
		c.Param("subscriptionId"),
	)
	writeMLModelGatewayResponse(c, response, problem)
}

func readMLModelGatewayBody(
	c *gin.Context,
	validate func([]byte) error,
) ([]byte, bool) {
	return readMLModelGatewayBodyWithMediaType(c, standardJSONMediaType, validate)
}

func readMLModelGatewayBodyWithMediaType(
	c *gin.Context,
	mediaType string,
	validate func([]byte) error,
) ([]byte, bool) {
	body, problem := readStandardBody(
		c,
		backend.MaxStandardMLModelBodyBytes,
		"ML model service body exceeds the configured transport limit",
		mediaType,
	)
	if problem != nil {
		util.GinProblemJson(c, problem)
		return nil, false
	}
	if err := validate(body); err != nil {
		util.GinProblemJson(c, malformedRequestProblem(err.Error()))
		return nil, false
	}
	return body, true
}

func writeMLModelGatewayResponse(
	c *gin.Context,
	response *backend.StandardResponse,
	problem *models.ProblemDetails,
) {
	if problem != nil {
		util.GinProblemJson(c, problem)
		return
	}
	if response == nil {
		writeMLModelGatewayUnavailable(c)
		return
	}
	if response.Location != "" {
		c.Header("Location", response.Location)
	}
	switch response.StatusCode {
	case http.StatusNoContent, http.StatusTemporaryRedirect, http.StatusPermanentRedirect:
		c.Status(response.StatusCode)
	default:
		c.Data(response.StatusCode, standardJSONMediaType, response.Body)
	}
}

func writeMLModelGatewayUnavailable(c *gin.Context) {
	util.GinProblemJson(c, &models.ProblemDetails{
		Status: http.StatusServiceUnavailable,
		Title:  http.StatusText(http.StatusServiceUnavailable),
		Detail: "ML model routing is temporarily unavailable",
	})
}
