package sbi

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/free5gc/nwdaf/internal/backend"
	wire "github.com/free5gc/nwdaf/internal/compat/mlmodeltraining"
	"github.com/free5gc/openapi/models"
)

type mlModelTrainingProcessor interface {
	HandleCreateMLModelTraining(context.Context, []byte) (*backend.StandardResponse, *models.ProblemDetails)
	HandleReplaceMLModelTraining(context.Context, string, []byte) (*backend.StandardResponse, *models.ProblemDetails)
	HandlePatchMLModelTraining(context.Context, string, []byte) (*backend.StandardResponse, *models.ProblemDetails)
	HandleDeleteMLModelTraining(context.Context, string) (*backend.StandardResponse, *models.ProblemDetails)
}

func (s *Server) getMLModelTrainingRoutes() []Route {
	return []Route{
		{
			Name: "CreateNWDAFMLModelTrainingSubscription", Method: http.MethodPost,
			Pattern: "/subscriptions", APIFunc: s.HandleCreateMLModelTraining,
		},
		{
			Name: "ReplaceNWDAFMLModelTrainingSubscription", Method: http.MethodPut,
			Pattern: "/subscriptions/:subscriptionId", APIFunc: s.HandleReplaceMLModelTraining,
		},
		{
			Name: "PatchNWDAFMLModelTrainingSubscription", Method: http.MethodPatch,
			Pattern: "/subscriptions/:subscriptionId", APIFunc: s.HandlePatchMLModelTraining,
		},
		{
			Name: "DeleteNWDAFMLModelTrainingSubscription", Method: http.MethodDelete,
			Pattern: "/subscriptions/:subscriptionId", APIFunc: s.HandleDeleteMLModelTraining,
		},
	}
}

func (s *Server) HandleCreateMLModelTraining(c *gin.Context) {
	body, ok := s.readMLModelBodyWithMediaTypeAndProblem(c, "application/json", func(body []byte) error {
		_, err := wire.ParseNwdafMLModelTrainSubsc(body)
		return err
	}, mlModelTrainingRequestProblem)
	if !ok {
		return
	}
	processor, ok := s.processor.(mlModelTrainingProcessor)
	if !ok {
		writeMLModelUnavailable(c)
		return
	}
	response, problem := processor.HandleCreateMLModelTraining(c.Request.Context(), body)
	writeMLModelResponse(c, response, problem)
}

func (s *Server) HandleReplaceMLModelTraining(c *gin.Context) {
	body, ok := s.readMLModelBodyWithMediaTypeAndProblem(c, "application/json", func(body []byte) error {
		_, err := wire.ParseNwdafMLModelTrainSubsc(body)
		return err
	}, mlModelTrainingRequestProblem)
	if !ok {
		return
	}
	processor, ok := s.processor.(mlModelTrainingProcessor)
	if !ok {
		writeMLModelUnavailable(c)
		return
	}
	response, problem := processor.HandleReplaceMLModelTraining(
		c.Request.Context(), c.Param("subscriptionId"), body,
	)
	writeMLModelResponse(c, response, problem)
}

func (s *Server) HandlePatchMLModelTraining(c *gin.Context) {
	body, ok := s.readMLModelBodyWithMediaTypeAndProblem(
		c, "application/merge-patch+json", func(body []byte) error {
			_, err := wire.ParseNwdafMLModelTrainSubscPatch(body)
			return err
		}, mlModelTrainingRequestProblem,
	)
	if !ok {
		return
	}
	processor, ok := s.processor.(mlModelTrainingProcessor)
	if !ok {
		writeMLModelUnavailable(c)
		return
	}
	response, problem := processor.HandlePatchMLModelTraining(
		c.Request.Context(), c.Param("subscriptionId"), body,
	)
	writeMLModelResponse(c, response, problem)
}

func mlModelTrainingRequestProblem(err error) *models.ProblemDetails {
	if problem, ok := wire.ProblemDetailsForValidation(err); ok {
		return problem
	}
	return nil
}

func (s *Server) HandleDeleteMLModelTraining(c *gin.Context) {
	processor, ok := s.processor.(mlModelTrainingProcessor)
	if !ok {
		writeMLModelUnavailable(c)
		return
	}
	response, problem := processor.HandleDeleteMLModelTraining(
		c.Request.Context(), c.Param("subscriptionId"),
	)
	writeMLModelResponse(c, response, problem)
}
