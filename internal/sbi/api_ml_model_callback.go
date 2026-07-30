package sbi

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/free5gc/nwdaf/internal/backend"
	wire "github.com/free5gc/nwdaf/internal/compat/mlmodel"
	trainingwire "github.com/free5gc/nwdaf/internal/compat/mlmodeltraining"
	"github.com/free5gc/openapi/models"
)

type mlModelCallbackProcessor interface {
	HandleMLModelProvisionNotification(
		context.Context,
		string,
		[]byte,
	) (*backend.StandardResponse, *models.ProblemDetails)
	HandleMLModelMonitorNotification(
		context.Context,
		string,
		[]byte,
	) (*backend.StandardResponse, *models.ProblemDetails)
	HandleMLModelTrainingNotification(
		context.Context,
		string,
		[]byte,
	) (*backend.StandardResponse, *models.ProblemDetails)
}

func (s *Server) getMLModelCallbackRoutes() []Route {
	return []Route{
		{
			Name:    "ReceiveMLModelProvisionNotification",
			Method:  http.MethodPost,
			Pattern: "/ml-model-provision/:localRouteId",
			APIFunc: s.HandleMLModelProvisionCallback,
		},
		{
			Name:    "ReceiveMLModelTrainingNotification",
			Method:  http.MethodPost,
			Pattern: "/ml-model-training/:localRouteId",
			APIFunc: s.HandleMLModelTrainingCallback,
		},
		{
			Name:    "ReceiveMLModelMonitorNotification",
			Method:  http.MethodPost,
			Pattern: "/ml-model-monitor/:localRouteId",
			APIFunc: s.HandleMLModelMonitorCallback,
		},
	}
}

func (s *Server) HandleMLModelTrainingCallback(c *gin.Context) {
	body, ok := s.readMLModelBody(c, func(body []byte) error {
		_, err := trainingwire.ParseNwdafMLModelTrainNotif(body)
		return err
	})
	if !ok {
		return
	}
	processor, ok := s.processor.(mlModelCallbackProcessor)
	if !ok {
		writeMLModelUnavailable(c)
		return
	}
	response, problem := processor.HandleMLModelTrainingNotification(
		c.Request.Context(), c.Param("localRouteId"), body,
	)
	writeMLModelResponse(c, response, problem)
}

func (s *Server) HandleMLModelProvisionCallback(c *gin.Context) {
	body, ok := s.readMLModelBody(c, func(body []byte) error {
		_, err := wire.ParseMLModelProvisionNotifications(body)
		return err
	})
	if !ok {
		return
	}
	processor, ok := s.processor.(mlModelCallbackProcessor)
	if !ok {
		writeMLModelUnavailable(c)
		return
	}
	response, problem := processor.HandleMLModelProvisionNotification(
		c.Request.Context(),
		c.Param("localRouteId"),
		body,
	)
	writeMLModelResponse(c, response, problem)
}

func (s *Server) HandleMLModelMonitorCallback(c *gin.Context) {
	body, ok := s.readMLModelBody(c, func(body []byte) error {
		_, err := wire.ParseMLModelMonitorNotification(body)
		return err
	})
	if !ok {
		return
	}
	processor, ok := s.processor.(mlModelCallbackProcessor)
	if !ok {
		writeMLModelUnavailable(c)
		return
	}
	response, problem := processor.HandleMLModelMonitorNotification(
		c.Request.Context(),
		c.Param("localRouteId"),
		body,
	)
	writeMLModelResponse(c, response, problem)
}
