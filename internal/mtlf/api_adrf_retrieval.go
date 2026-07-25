package mtlf

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"

	"github.com/gin-gonic/gin"

	mtlfprocessor "github.com/free5gc/nwdaf/internal/mtlf/processor"
	"github.com/free5gc/nwdaf/internal/sbi/consumer"
	"github.com/free5gc/nwdaf/internal/util"
	"github.com/free5gc/openapi/models"
)

const maxAdrfControlBodyBytes = 4 * 1024 * 1024

type adrfRetrievalProcessor interface {
	CreateAdrfRetrievalSubscription(context.Context, string, []byte) (*consumer.StandardAdrfResponse, error)
	DeleteAdrfRetrievalSubscription(context.Context, string) (*consumer.StandardAdrfResponse, error)
}

func (s *Server) adrfRetrievalRoutes() []Route {
	return []Route{
		{
			Name:    "CreateAdrfRetrievalSubscription",
			Method:  http.MethodPost,
			Pattern: "/internal/v1/adrf-data-management/data-retrieval-subscriptions",
			APIFunc: s.CreateAdrfRetrievalSubscription,
		},
		{
			Name:    "DeleteAdrfRetrievalSubscription",
			Method:  http.MethodDelete,
			Pattern: "/internal/v1/adrf-data-management/data-retrieval-subscriptions/:subscriptionId",
			APIFunc: s.DeleteAdrfRetrievalSubscription,
		},
	}
}

func (s *Server) CreateAdrfRetrievalSubscription(c *gin.Context) {
	body, problem := readStandardJSONBody(
		c,
		maxAdrfControlBodyBytes,
		"ADRF retrieval body exceeds limit",
	)
	if problem != nil {
		util.GinProblemJson(c, problem)
		return
	}
	var subscription consumer.NadrfDataRetrievalSubscription
	if err := json.Unmarshal(body, &subscription); err != nil ||
		subscription.NotifCorrId == "" || subscription.NotificationURI == "" ||
		subscription.DataSub.SmfDataSub == nil || subscription.TimePeriod.StartTime == "" ||
		subscription.TimePeriod.StopTime == "" || !subscription.ConsTrigNotif {
		util.GinProblemJson(c, malformedRequestProblem("invalid NadrfDataRetrievalSubscription"))
		return
	}
	if s.publicCallbackBaseURI != "" &&
		subscription.NotificationURI != s.publicCallbackBaseURI+"/collector/retrieval-notify" {
		util.GinProblemJson(c, malformedRequestProblem(
			"notificationURI must be the containing NWDAF ADRF callback",
		))
		return
	}
	target := strings.TrimRight(strings.TrimSpace(c.GetHeader("Target-Api-Root")), "/")
	parsed, err := url.Parse(target)
	if err != nil || parsed.Scheme != "http" && parsed.Scheme != "https" ||
		parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" ||
		parsed.Fragment != "" || parsed.Path != "" {
		util.GinProblemJson(c, malformedRequestProblem("Target-Api-Root must be an HTTP(S) origin"))
		return
	}
	processor, ok := s.processor.(adrfRetrievalProcessor)
	if !ok {
		util.GinProblemJson(c, adrfRetrievalUnavailableProblem())
		return
	}
	response, requestErr := processor.CreateAdrfRetrievalSubscription(
		c.Request.Context(),
		target,
		body,
	)
	writeAdrfControlResponse(c, response, requestErr)
}

func (s *Server) DeleteAdrfRetrievalSubscription(c *gin.Context) {
	processor, ok := s.processor.(adrfRetrievalProcessor)
	if !ok {
		util.GinProblemJson(c, adrfRetrievalUnavailableProblem())
		return
	}
	response, err := processor.DeleteAdrfRetrievalSubscription(
		c.Request.Context(),
		c.Param("subscriptionId"),
	)
	if errors.Is(err, mtlfprocessor.ErrAdrfRetrievalRouteNotFound) {
		util.GinProblemJson(c, &models.ProblemDetails{
			Status: http.StatusNotFound,
			Title:  http.StatusText(http.StatusNotFound),
			Cause:  "RESOURCE_NOT_FOUND",
			Detail: "ADRF retrieval route was not found",
		})
		return
	}
	writeAdrfControlResponse(c, response, err)
}

func writeAdrfControlResponse(
	c *gin.Context,
	response *consumer.StandardAdrfResponse,
	err error,
) {
	if err == nil && response != nil {
		if response.Location != "" {
			c.Header("Location", response.Location)
		}
		if len(response.Body) == 0 {
			c.Status(response.StatusCode)
		} else {
			c.Data(response.StatusCode, response.ContentType, response.Body)
		}
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
		Detail: "ADRF retrieval control request failed",
	})
}

func adrfRetrievalUnavailableProblem() *models.ProblemDetails {
	return &models.ProblemDetails{
		Status: http.StatusServiceUnavailable,
		Title:  http.StatusText(http.StatusServiceUnavailable),
		Detail: "ADRF retrieval routing is temporarily unavailable",
	}
}
