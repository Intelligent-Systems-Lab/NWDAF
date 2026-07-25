package anlf

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"

	"github.com/gin-gonic/gin"

	anlfprocessor "github.com/free5gc/nwdaf/internal/anlf/processor"
	"github.com/free5gc/nwdaf/internal/compat/nsmf"
	"github.com/free5gc/nwdaf/internal/sbi/consumer"
	"github.com/free5gc/nwdaf/internal/util"
	"github.com/free5gc/openapi/models"
)

const maxSmfEventExposureBodyBytes = 4 * 1024 * 1024

type smfEventExposureProcessor interface {
	CreateSmfEventExposure(context.Context, string, []byte) (*consumer.StandardSmfResponse, error)
	ReadSmfEventExposure(context.Context, string, string) (*consumer.StandardSmfResponse, error)
	ReplaceSmfEventExposure(context.Context, string, string, []byte) (*consumer.StandardSmfResponse, error)
	DeleteSmfEventExposure(context.Context, string, string) (*consumer.StandardSmfResponse, error)
}

func (s *Server) smfEventExposureRoutes() []Route {
	return []Route{
		{
			Name:    "CreateSmfEventExposure",
			Method:  http.MethodPost,
			Pattern: "/internal/v1/smf-event-exposure/subscriptions",
			APIFunc: s.CreateSmfEventExposure,
		},
		{
			Name:    "ReadSmfEventExposure",
			Method:  http.MethodGet,
			Pattern: "/internal/v1/smf-event-exposure/subscriptions/:subscriptionId",
			APIFunc: s.ReadSmfEventExposure,
		},
		{
			Name:    "ReplaceSmfEventExposure",
			Method:  http.MethodPut,
			Pattern: "/internal/v1/smf-event-exposure/subscriptions/:subscriptionId",
			APIFunc: s.ReplaceSmfEventExposure,
		},
		{
			Name:    "DeleteSmfEventExposure",
			Method:  http.MethodDelete,
			Pattern: "/internal/v1/smf-event-exposure/subscriptions/:subscriptionId",
			APIFunc: s.DeleteSmfEventExposure,
		},
	}
}

func (s *Server) CreateSmfEventExposure(c *gin.Context) {
	targetAPIBaseURI := c.GetHeader("Target-Api-Root")
	if !validTargetAPIBaseURI(targetAPIBaseURI) {
		util.GinProblemJson(c, &models.ProblemDetails{
			Status: http.StatusBadRequest,
			Title:  http.StatusText(http.StatusBadRequest),
			Cause:  "INVALID_REQUEST",
			Detail: "Target-Api-Root must be an absolute HTTP(S) API root",
		})
		return
	}
	body, problem := readSmfEventExposureBody(c)
	if problem != nil {
		util.GinProblemJson(c, problem)
		return
	}
	processor, ok := s.processor.(smfEventExposureProcessor)
	if !ok {
		util.GinProblemJson(c, smfEventExposureUnavailableProblem())
		return
	}
	response, err := processor.CreateSmfEventExposure(c.Request.Context(), targetAPIBaseURI, body)
	s.writeSmfEventExposureResponse(c, response, err, false)
}

func (s *Server) ReadSmfEventExposure(c *gin.Context) {
	targetAPIBaseURI, ok := readTargetAPIBaseURI(c)
	if !ok {
		return
	}
	processor, ok := s.processor.(smfEventExposureProcessor)
	if !ok {
		util.GinProblemJson(c, smfEventExposureUnavailableProblem())
		return
	}
	response, err := processor.ReadSmfEventExposure(
		c.Request.Context(),
		targetAPIBaseURI,
		c.Param("subscriptionId"),
	)
	s.writeSmfEventExposureResponse(c, response, err, true)
}

func (s *Server) ReplaceSmfEventExposure(c *gin.Context) {
	targetAPIBaseURI, ok := readTargetAPIBaseURI(c)
	if !ok {
		return
	}
	body, problem := readSmfEventExposureBody(c)
	if problem != nil {
		util.GinProblemJson(c, problem)
		return
	}
	processor, ok := s.processor.(smfEventExposureProcessor)
	if !ok {
		util.GinProblemJson(c, smfEventExposureUnavailableProblem())
		return
	}
	response, err := processor.ReplaceSmfEventExposure(
		c.Request.Context(),
		targetAPIBaseURI,
		c.Param("subscriptionId"),
		body,
	)
	s.writeSmfEventExposureResponse(c, response, err, true)
}

func (s *Server) DeleteSmfEventExposure(c *gin.Context) {
	targetAPIBaseURI, ok := readTargetAPIBaseURI(c)
	if !ok {
		return
	}
	processor, ok := s.processor.(smfEventExposureProcessor)
	if !ok {
		util.GinProblemJson(c, smfEventExposureUnavailableProblem())
		return
	}
	response, err := processor.DeleteSmfEventExposure(
		c.Request.Context(),
		targetAPIBaseURI,
		c.Param("subscriptionId"),
	)
	s.writeSmfEventExposureResponse(c, response, err, true)
}

func readTargetAPIBaseURI(c *gin.Context) (string, bool) {
	targetAPIBaseURI := c.GetHeader("Target-Api-Root")
	if validTargetAPIBaseURI(targetAPIBaseURI) {
		return targetAPIBaseURI, true
	}
	util.GinProblemJson(c, &models.ProblemDetails{
		Status: http.StatusBadRequest,
		Title:  http.StatusText(http.StatusBadRequest),
		Cause:  "INVALID_REQUEST",
		Detail: "Target-Api-Root must be an absolute HTTP(S) API root",
	})
	return "", false
}

func readSmfEventExposureBody(c *gin.Context) ([]byte, *models.ProblemDetails) {
	body, problem := readStandardJSONBody(
		c,
		maxSmfEventExposureBodyBytes,
		"SMF Event Exposure body exceeds the configured transport limit",
	)
	if problem != nil {
		return nil, problem
	}
	var representation models.NsmfEventExposure
	if err := json.Unmarshal(body, &representation); err != nil {
		return nil, malformedRequestProblem(err.Error())
	}
	if representation.NotifId == "" || representation.NotifUri == "" || len(representation.EventSubs) == 0 {
		return nil, &models.ProblemDetails{
			Status: http.StatusBadRequest,
			Title:  http.StatusText(http.StatusBadRequest),
			Cause:  "MANDATORY_IE_MISSING",
			Detail: "notifId, notifUri and eventSubs are required",
		}
	}
	var release18Fields nsmf.EventExposure
	if err := json.Unmarshal(body, &release18Fields); err != nil {
		return nil, malformedRequestProblem(err.Error())
	}
	for _, eventSubscription := range release18Fields.EventSubs {
		if eventSubscription.Event == nsmf.EventUPFEvent && release18Fields.NFID == "" {
			return nil, &models.ProblemDetails{
				Status: http.StatusBadRequest,
				Title:  http.StatusText(http.StatusBadRequest),
				Cause:  "MANDATORY_IE_MISSING",
				Detail: "nfId is required for an UPF_EVENT subscription",
			}
		}
	}
	return body, nil
}

func (s *Server) writeSmfEventExposureResponse(
	c *gin.Context,
	response *consumer.StandardSmfResponse,
	err error,
	allowRedirect bool,
) {
	if response != nil {
		if response.Location != "" {
			c.Header("Location", response.Location)
		}
		if err == nil || allowRedirect && (response.StatusCode == http.StatusTemporaryRedirect ||
			response.StatusCode == http.StatusPermanentRedirect) {
			if len(response.Body) == 0 {
				c.Status(response.StatusCode)
				return
			}
			contentType := response.ContentType
			if contentType == "" {
				contentType = "application/json"
			}
			c.Data(response.StatusCode, contentType, response.Body)
			return
		}
	}
	if response != nil && !allowRedirect &&
		(response.StatusCode == http.StatusTemporaryRedirect || response.StatusCode == http.StatusPermanentRedirect) {
		util.GinProblemJson(c, &models.ProblemDetails{
			Status: http.StatusBadGateway,
			Title:  http.StatusText(http.StatusBadGateway),
			Detail: "SMF Event Exposure create returned an undeclared redirect response",
		})
		return
	}
	if errors.Is(err, anlfprocessor.ErrSmfEventExposureUnavailable) || response == nil && err == nil {
		util.GinProblemJson(c, smfEventExposureUnavailableProblem())
		return
	}
	var standardError interface {
		StandardProblemDetails() *models.ProblemDetails
	}
	if errors.As(err, &standardError) {
		util.GinProblemJson(c, standardError.StandardProblemDetails())
		return
	}
	util.GinProblemJson(c, &models.ProblemDetails{
		Status: http.StatusBadGateway,
		Title:  http.StatusText(http.StatusBadGateway),
		Detail: "SMF Event Exposure request failed",
	})
}

func validTargetAPIBaseURI(value string) bool {
	parsed, err := url.ParseRequestURI(value)
	return err == nil && parsed.Host != "" && parsed.User == nil && parsed.RawQuery == "" &&
		parsed.Fragment == "" && (parsed.Scheme == "http" || parsed.Scheme == "https")
}

func smfEventExposureUnavailableProblem() *models.ProblemDetails {
	return &models.ProblemDetails{
		Status: http.StatusServiceUnavailable,
		Title:  http.StatusText(http.StatusServiceUnavailable),
		Detail: "SMF Event Exposure transport is temporarily unavailable",
	}
}
