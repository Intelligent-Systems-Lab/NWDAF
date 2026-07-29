package anlf

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/free5gc/nwdaf/internal/backend"
	wire "github.com/free5gc/nwdaf/internal/compat/mlmodel"
	"github.com/free5gc/nwdaf/internal/util"
	"github.com/free5gc/openapi/models"
)

func (s *Server) anlfMLModelRoutes() []Route {
	return []Route{
		{
			Name: "CreateMLModelProvisionFromBackend", Method: http.MethodPost,
			Pattern: "/internal/v1/ml-model-provision/subscriptions",
			APIFunc: s.HandleCreateMLModelProvisionFromBackend,
		},
		{
			Name: "ReplaceMLModelProvisionFromBackend", Method: http.MethodPut,
			Pattern: "/internal/v1/ml-model-provision/subscriptions/:subscriptionId",
			APIFunc: s.HandleReplaceMLModelProvisionFromBackend,
		},
		{
			Name: "DeleteMLModelProvisionFromBackend", Method: http.MethodDelete,
			Pattern: "/internal/v1/ml-model-provision/subscriptions/:subscriptionId",
			APIFunc: s.HandleDeleteMLModelProvisionFromBackend,
		},
		{
			Name: "CreateMLModelMonitorRegistrationFromBackend", Method: http.MethodPost,
			Pattern: "/internal/v1/ml-model-monitor/registrations",
			APIFunc: s.HandleCreateMLModelMonitorRegistrationFromBackend,
		},
		{
			Name: "DeleteMLModelMonitorRegistrationFromBackend", Method: http.MethodDelete,
			Pattern: "/internal/v1/ml-model-monitor/registrations/:registrationId",
			APIFunc: s.HandleDeleteMLModelMonitorRegistrationFromBackend,
		},
		{
			Name: "MLModelMonitorNotification", Method: http.MethodPost,
			Pattern: "/internal/v1/ml-model-monitor/notifications",
			APIFunc: s.HandleMLModelMonitorNotification,
		},
		{
			Name: "MLModelMonitorResourceNotification", Method: http.MethodPost,
			Pattern: "/internal/v1/ml-model-monitor/subscriptions/:subscriptionId/notifications",
			APIFunc: s.HandleMLModelMonitorNotification,
		},
	}
}

func (s *Server) HandleCreateMLModelProvisionFromBackend(c *gin.Context) {
	body, ok := readMLModelGatewayBody(c, func(body []byte) error {
		_, err := wire.ParseMLModelProvisionSubscription(body)
		return err
	})
	if !ok {
		return
	}
	if s.mlModel == nil {
		writeMLModelGatewayUnavailable(c)
		return
	}
	target, err := backend.ParseSelectedTargetHeaders(
		c.Request.Header,
		"nnwdaf-mlmodelprovision",
	)
	if err != nil {
		util.GinProblemJson(c, malformedRequestProblem(err.Error()))
		return
	}
	response, problem := s.mlModel.HandleCreateMLModelProvisionFromBackend(
		c.Request.Context(),
		body,
		target,
	)
	writeMLModelGatewayResponse(c, response, problem)
}

func (s *Server) HandleReplaceMLModelProvisionFromBackend(c *gin.Context) {
	body, ok := readMLModelGatewayBody(c, func(body []byte) error {
		_, err := wire.ParseMLModelProvisionSubscription(body)
		return err
	})
	if !ok {
		return
	}
	if s.mlModel == nil {
		writeMLModelGatewayUnavailable(c)
		return
	}
	response, problem := s.mlModel.HandleReplaceMLModelProvisionFromBackend(
		c.Request.Context(),
		c.Param("subscriptionId"),
		body,
	)
	writeMLModelGatewayResponse(c, response, problem)
}

func (s *Server) HandleDeleteMLModelProvisionFromBackend(c *gin.Context) {
	if s.mlModel == nil {
		writeMLModelGatewayUnavailable(c)
		return
	}
	response, problem := s.mlModel.HandleDeleteMLModelProvisionFromBackend(
		c.Request.Context(),
		c.Param("subscriptionId"),
	)
	writeMLModelGatewayResponse(c, response, problem)
}

func (s *Server) HandleCreateMLModelMonitorRegistrationFromBackend(c *gin.Context) {
	body, ok := readMLModelGatewayBody(c, func(body []byte) error {
		_, err := wire.ParseMLModelMonitorRegistration(body)
		return err
	})
	if !ok {
		return
	}
	if s.mlModel == nil {
		writeMLModelGatewayUnavailable(c)
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
	response, problem := s.mlModel.HandleCreateMLModelMonitorRegistrationFromBackend(
		c.Request.Context(),
		body,
		target,
	)
	writeMLModelGatewayResponse(c, response, problem)
}

func (s *Server) HandleDeleteMLModelMonitorRegistrationFromBackend(c *gin.Context) {
	if s.mlModel == nil {
		writeMLModelGatewayUnavailable(c)
		return
	}
	response, problem := s.mlModel.HandleDeleteMLModelMonitorRegistrationFromBackend(
		c.Request.Context(),
		c.Param("registrationId"),
	)
	writeMLModelGatewayResponse(c, response, problem)
}

func (s *Server) HandleMLModelMonitorNotification(c *gin.Context) {
	body, ok := readMLModelGatewayBody(c, func(body []byte) error {
		_, err := wire.ParseMLModelMonitorNotification(body)
		return err
	})
	if !ok {
		return
	}
	if s.mlModel == nil {
		writeMLModelGatewayUnavailable(c)
		return
	}
	response, problem := s.mlModel.HandleMLModelMonitorNotification(
		c.Request.Context(),
		c.Param("subscriptionId"),
		body,
	)
	writeMLModelGatewayResponse(c, response, problem)
}

func readMLModelGatewayBody(
	c *gin.Context,
	validate func([]byte) error,
) ([]byte, bool) {
	body, problem := readStandardJSONBody(
		c,
		backend.MaxStandardMLModelBodyBytes,
		"ML model service body exceeds the configured transport limit",
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
		c.Data(response.StatusCode, "application/json", response.Body)
	}
}

func writeMLModelGatewayUnavailable(c *gin.Context) {
	util.GinProblemJson(c, &models.ProblemDetails{
		Status: http.StatusServiceUnavailable,
		Title:  http.StatusText(http.StatusServiceUnavailable),
		Detail: "ML model routing is temporarily unavailable",
	})
}
