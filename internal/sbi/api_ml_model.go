package sbi

import (
	"context"
	"errors"
	"io"
	"mime"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/free5gc/nwdaf/internal/backend"
	wire "github.com/free5gc/nwdaf/internal/compat/mlmodel"
	"github.com/free5gc/nwdaf/internal/util"
	"github.com/free5gc/openapi"
	"github.com/free5gc/openapi/models"
)

const mlModelMonitorServiceName models.ServiceName = "nnwdaf-mlmodelmonitor"

type mlModelProcessor interface {
	HandleCreateMLModelProvision(context.Context, []byte) (*backend.StandardResponse, *models.ProblemDetails)
	HandleReplaceMLModelProvision(context.Context, string, []byte) (*backend.StandardResponse, *models.ProblemDetails)
	HandleDeleteMLModelProvision(context.Context, string) (*backend.StandardResponse, *models.ProblemDetails)
	HandleCreateMLModelMonitorRegistration(context.Context, []byte) (*backend.StandardResponse, *models.ProblemDetails)
	HandleDeleteMLModelMonitorRegistration(context.Context, string) (*backend.StandardResponse, *models.ProblemDetails)
	HandleCreateMLModelMonitorSubscription(context.Context, []byte) (*backend.StandardResponse, *models.ProblemDetails)
	HandleReplaceMLModelMonitorSubscription(
		context.Context,
		string,
		[]byte,
	) (*backend.StandardResponse, *models.ProblemDetails)
	HandleDeleteMLModelMonitorSubscription(context.Context, string) (*backend.StandardResponse, *models.ProblemDetails)
}

func (s *Server) getMLModelProvisionRoutes() []Route {
	return []Route{
		{
			Name: "CreateNWDAFMLModelProvisionSubscription", Method: http.MethodPost,
			Pattern: "/subscriptions", APIFunc: s.HandleCreateMLModelProvision,
		},
		{
			Name: "ReplaceNWDAFMLModelProvisionSubscription", Method: http.MethodPut,
			Pattern: "/subscriptions/:subscriptionId", APIFunc: s.HandleReplaceMLModelProvision,
		},
		{
			Name: "DeleteNWDAFMLModelProvisionSubscription", Method: http.MethodDelete,
			Pattern: "/subscriptions/:subscriptionId", APIFunc: s.HandleDeleteMLModelProvision,
		},
	}
}

func (s *Server) getMLModelMonitorRoutes() []Route {
	return []Route{
		{
			Name: "CreateNWDAFMLModelMonitoringRegistration", Method: http.MethodPost,
			Pattern: "/registrations", APIFunc: s.HandleCreateMLModelMonitorRegistration,
		},
		{
			Name: "DeleteNWDAFMLModelMonitoringRegistration", Method: http.MethodDelete,
			Pattern: "/registrations/:registrationId", APIFunc: s.HandleDeleteMLModelMonitorRegistration,
		},
		{
			Name: "CreateNWDAFMLModelMonitoringSubscription", Method: http.MethodPost,
			Pattern: "/subscriptions", APIFunc: s.HandleCreateMLModelMonitorSubscription,
		},
		{
			Name: "ReplaceNWDAFMLModelMonitoringSubscription", Method: http.MethodPut,
			Pattern: "/subscriptions/:subscriptionId", APIFunc: s.HandleReplaceMLModelMonitorSubscription,
		},
		{
			Name: "DeleteNWDAFMLModelMonitoringSubscription", Method: http.MethodDelete,
			Pattern: "/subscriptions/:subscriptionId", APIFunc: s.HandleDeleteMLModelMonitorSubscription,
		},
	}
}

func (s *Server) HandleCreateMLModelProvision(c *gin.Context) {
	body, ok := s.readMLModelBody(c, func(body []byte) error {
		_, err := wire.ParseMLModelProvisionSubscription(body)
		return err
	})
	if !ok {
		return
	}
	processor, ok := s.processor.(mlModelProcessor)
	if !ok {
		writeMLModelUnavailable(c)
		return
	}
	response, problem := processor.HandleCreateMLModelProvision(c.Request.Context(), body)
	writeMLModelResponse(c, response, problem)
}

func (s *Server) HandleReplaceMLModelProvision(c *gin.Context) {
	body, ok := s.readMLModelBody(c, func(body []byte) error {
		_, err := wire.ParseMLModelProvisionSubscription(body)
		return err
	})
	if !ok {
		return
	}
	processor, ok := s.processor.(mlModelProcessor)
	if !ok {
		writeMLModelUnavailable(c)
		return
	}
	response, problem := processor.HandleReplaceMLModelProvision(
		c.Request.Context(), c.Param("subscriptionId"), body,
	)
	writeMLModelResponse(c, response, problem)
}

func (s *Server) HandleDeleteMLModelProvision(c *gin.Context) {
	processor, ok := s.processor.(mlModelProcessor)
	if !ok {
		writeMLModelUnavailable(c)
		return
	}
	response, problem := processor.HandleDeleteMLModelProvision(
		c.Request.Context(), c.Param("subscriptionId"),
	)
	writeMLModelResponse(c, response, problem)
}

func (s *Server) HandleCreateMLModelMonitorRegistration(c *gin.Context) {
	body, ok := s.readMLModelBody(c, func(body []byte) error {
		_, err := wire.ParseMLModelMonitorRegistration(body)
		return err
	})
	if !ok {
		return
	}
	processor, ok := s.processor.(mlModelProcessor)
	if !ok {
		writeMLModelUnavailable(c)
		return
	}
	response, problem := processor.HandleCreateMLModelMonitorRegistration(c.Request.Context(), body)
	writeMLModelResponse(c, response, problem)
}

func (s *Server) HandleDeleteMLModelMonitorRegistration(c *gin.Context) {
	processor, ok := s.processor.(mlModelProcessor)
	if !ok {
		writeMLModelUnavailable(c)
		return
	}
	response, problem := processor.HandleDeleteMLModelMonitorRegistration(
		c.Request.Context(), c.Param("registrationId"),
	)
	writeMLModelResponse(c, response, problem)
}

func (s *Server) HandleCreateMLModelMonitorSubscription(c *gin.Context) {
	body, ok := s.readMLModelBody(c, func(body []byte) error {
		_, err := wire.ParseMLModelMonitorSubscription(body)
		return err
	})
	if !ok {
		return
	}
	processor, ok := s.processor.(mlModelProcessor)
	if !ok {
		writeMLModelUnavailable(c)
		return
	}
	response, problem := processor.HandleCreateMLModelMonitorSubscription(c.Request.Context(), body)
	writeMLModelResponse(c, response, problem)
}

func (s *Server) HandleReplaceMLModelMonitorSubscription(c *gin.Context) {
	body, ok := s.readMLModelBody(c, func(body []byte) error {
		_, err := wire.ParseMLModelMonitorSubscription(body)
		return err
	})
	if !ok {
		return
	}
	processor, ok := s.processor.(mlModelProcessor)
	if !ok {
		writeMLModelUnavailable(c)
		return
	}
	response, problem := processor.HandleReplaceMLModelMonitorSubscription(
		c.Request.Context(), c.Param("subscriptionId"), body,
	)
	writeMLModelResponse(c, response, problem)
}

func (s *Server) HandleDeleteMLModelMonitorSubscription(c *gin.Context) {
	processor, ok := s.processor.(mlModelProcessor)
	if !ok {
		writeMLModelUnavailable(c)
		return
	}
	response, problem := processor.HandleDeleteMLModelMonitorSubscription(
		c.Request.Context(), c.Param("subscriptionId"),
	)
	writeMLModelResponse(c, response, problem)
}

func (s *Server) readMLModelBody(c *gin.Context, validate func([]byte) error) ([]byte, bool) {
	return s.readMLModelBodyWithMediaType(c, "application/json", validate)
}

func (s *Server) readMLModelBodyWithMediaType(
	c *gin.Context,
	expectedMediaType string,
	validate func([]byte) error,
) ([]byte, bool) {
	mediaType, _, err := mime.ParseMediaType(c.GetHeader("Content-Type"))
	if err != nil || mediaType != expectedMediaType {
		util.GinProblemJson(c, &models.ProblemDetails{
			Status: http.StatusUnsupportedMediaType,
			Title:  http.StatusText(http.StatusUnsupportedMediaType),
			Cause:  "UNSUPPORTED_MEDIA_TYPE",
			Detail: "Content-Type must be " + expectedMediaType,
		})
		return nil, false
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, backend.MaxStandardMLModelBodyBytes)
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		var maxBytesError *http.MaxBytesError
		if errors.As(err, &maxBytesError) {
			util.GinProblemJson(c, &models.ProblemDetails{
				Status: http.StatusRequestEntityTooLarge,
				Title:  http.StatusText(http.StatusRequestEntityTooLarge),
				Cause:  "REQUEST_TOO_LARGE",
				Detail: "ML model service body exceeds the configured transport limit",
			})
			return nil, false
		}
		util.GinProblemJson(c, &models.ProblemDetails{
			Status: http.StatusInternalServerError,
			Title:  http.StatusText(http.StatusInternalServerError),
			Detail: "could not read ML model service body",
		})
		return nil, false
	}
	if validateErr := validate(body); validateErr != nil {
		util.GinProblemJson(c, openapi.ProblemDetailsMalformedReqSyntax(validateErr.Error()))
		return nil, false
	}
	return body, true
}

func writeMLModelResponse(
	c *gin.Context,
	response *backend.StandardResponse,
	problem *models.ProblemDetails,
) {
	if problem != nil {
		util.GinProblemJson(c, problem)
		return
	}
	if response == nil {
		writeMLModelUnavailable(c)
		return
	}
	if response.Location != "" {
		c.Header("Location", response.Location)
	}
	if response.StatusCode == http.StatusNoContent ||
		response.StatusCode == http.StatusTemporaryRedirect ||
		response.StatusCode == http.StatusPermanentRedirect {
		c.Status(response.StatusCode)
		return
	}
	c.Data(response.StatusCode, "application/json", response.Body)
}

func writeMLModelUnavailable(c *gin.Context) {
	util.GinProblemJson(c, &models.ProblemDetails{
		Status: http.StatusServiceUnavailable,
		Title:  http.StatusText(http.StatusServiceUnavailable),
		Detail: "requested ML model service capability is temporarily unavailable",
	})
}
