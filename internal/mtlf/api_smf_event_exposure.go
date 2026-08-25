package mtlf

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/free5gc/nwdaf/internal/compat/nsmf"
	mtlfprocessor "github.com/free5gc/nwdaf/internal/mtlf/processor"
	"github.com/free5gc/nwdaf/internal/sbi/consumer"
	"github.com/free5gc/nwdaf/internal/util"
	"github.com/free5gc/openapi/models"
)

const (
	maxSmfEventExposureBodyBytes = 4 * 1024 * 1024
	targetHTTPSScheme            = "https"
)

type smfEventExposureProcessor interface {
	CreateSmfEventExposure(context.Context, string, []byte) (*consumer.StandardSmfResponse, error)
	ReadSmfEventExposure(context.Context, string, string) (*consumer.StandardSmfResponse, error)
	ReplaceSmfEventExposure(context.Context, string, string, []byte) (*consumer.StandardSmfResponse, error)
	DeleteSmfEventExposure(context.Context, string, string) (*consumer.StandardSmfResponse, error)
}

func (s *Server) smfEventExposureRoutes() []Route {
	resource := "/internal/v1/smf-event-exposure/subscriptions/:subscriptionId"
	return []Route{
		{
			Name:    "CreateSmfEventExposure",
			Method:  http.MethodPost,
			Pattern: "/internal/v1/smf-event-exposure/subscriptions",
			APIFunc: s.CreateSmfEventExposure,
		},
		{Name: "ReadSmfEventExposure", Method: http.MethodGet, Pattern: resource, APIFunc: s.ReadSmfEventExposure},
		{Name: "ReplaceSmfEventExposure", Method: http.MethodPut, Pattern: resource, APIFunc: s.ReplaceSmfEventExposure},
		{Name: "DeleteSmfEventExposure", Method: http.MethodDelete, Pattern: resource, APIFunc: s.DeleteSmfEventExposure},
	}
}

func (s *Server) CreateSmfEventExposure(c *gin.Context) {
	target, ok := readCollectionTargetAPIBaseURI(c)
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
	response, err := processor.CreateSmfEventExposure(c.Request.Context(), target, body)
	s.writeSmfEventExposureResponse(c, response, err, false)
}

func (s *Server) ReadSmfEventExposure(c *gin.Context) {
	s.handleSmfEventExposureWithoutBody(c, http.MethodGet)
}

func (s *Server) DeleteSmfEventExposure(c *gin.Context) {
	s.handleSmfEventExposureWithoutBody(c, http.MethodDelete)
}

func (s *Server) handleSmfEventExposureWithoutBody(c *gin.Context, method string) {
	target, ok := readCollectionTargetAPIBaseURI(c)
	if !ok {
		return
	}
	processor, ok := s.processor.(smfEventExposureProcessor)
	if !ok {
		util.GinProblemJson(c, smfEventExposureUnavailableProblem())
		return
	}
	var response *consumer.StandardSmfResponse
	var err error
	if method == http.MethodGet {
		response, err = processor.ReadSmfEventExposure(c.Request.Context(), target, c.Param("subscriptionId"))
	} else {
		response, err = processor.DeleteSmfEventExposure(c.Request.Context(), target, c.Param("subscriptionId"))
	}
	s.writeSmfEventExposureResponse(c, response, err, true)
}

func (s *Server) ReplaceSmfEventExposure(c *gin.Context) {
	target, ok := readCollectionTargetAPIBaseURI(c)
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
		c.Request.Context(), target, c.Param("subscriptionId"), body,
	)
	s.writeSmfEventExposureResponse(c, response, err, true)
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
	var release18 nsmf.EventExposure
	if err := json.Unmarshal(body, &release18); err != nil {
		return nil, malformedRequestProblem(err.Error())
	}
	for _, eventSubscription := range release18.EventSubs {
		if eventSubscription.Event == nsmf.EventUPFEvent && release18.NFID == "" {
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
		redirect := response.StatusCode == http.StatusTemporaryRedirect || response.StatusCode == http.StatusPermanentRedirect
		provisionalCreate := !allowRedirect && response.ProvisionalResource &&
			response.StatusCode == http.StatusCreated && response.Location != ""
		if err == nil || allowRedirect && redirect || provisionalCreate {
			if len(response.Body) == 0 {
				c.Status(response.StatusCode)
				return
			}
			contentType := response.ContentType
			if contentType == "" {
				contentType = standardJSONMediaType
			}
			c.Data(response.StatusCode, contentType, response.Body)
			return
		}
		if !allowRedirect && redirect {
			util.GinProblemJson(c, &models.ProblemDetails{
				Status: http.StatusBadGateway,
				Title:  http.StatusText(http.StatusBadGateway),
				Detail: "SMF Event Exposure create returned an undeclared redirect response",
			})
			return
		}
	}
	if errors.Is(err, mtlfprocessor.ErrSmfEventExposureUnavailable) || response == nil && err == nil {
		util.GinProblemJson(c, smfEventExposureUnavailableProblem())
		return
	}
	var standardError interface{ StandardProblemDetails() *models.ProblemDetails }
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

func readCollectionTargetAPIBaseURI(c *gin.Context) (string, bool) {
	value := strings.TrimSpace(c.GetHeader("Target-Api-Root"))
	parsed, err := url.ParseRequestURI(value)
	valid := err == nil && parsed.Host != "" && parsed.User == nil && parsed.RawQuery == "" &&
		parsed.Fragment == "" && (parsed.Path == "" || parsed.Path == "/") &&
		(parsed.Scheme == "http" || parsed.Scheme == targetHTTPSScheme)
	if valid {
		return strings.TrimRight(value, "/"), true
	}
	util.GinProblemJson(c, &models.ProblemDetails{
		Status: http.StatusBadRequest,
		Title:  http.StatusText(http.StatusBadRequest),
		Cause:  "INVALID_REQUEST",
		Detail: "Target-Api-Root must be an HTTP(S) origin",
	})
	return "", false
}

func smfEventExposureUnavailableProblem() *models.ProblemDetails {
	return &models.ProblemDetails{
		Status: http.StatusServiceUnavailable,
		Title:  http.StatusText(http.StatusServiceUnavailable),
		Detail: "SMF Event Exposure transport is temporarily unavailable",
	}
}
