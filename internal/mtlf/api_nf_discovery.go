package mtlf

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	mtlfprocessor "github.com/free5gc/nwdaf/internal/mtlf/processor"
	"github.com/free5gc/nwdaf/internal/sbi/consumer"
	"github.com/free5gc/nwdaf/internal/util"
	"github.com/free5gc/openapi/models"
)

type nfDiscoveryProcessor interface {
	HandleNFDiscovery(context.Context, consumer.NFDiscoveryQuery) (*consumer.NFDiscoveryResult, error)
}

func (s *Server) nfDiscoveryRoutes() []Route {
	return []Route{{
		Name:    "HandleNFDiscovery",
		Method:  http.MethodGet,
		Pattern: "/internal/v1/nrf/nf-instances",
		APIFunc: s.HandleNFDiscovery,
	}}
}

func (s *Server) HandleNFDiscovery(c *gin.Context) {
	query, problem := validateNFDiscoveryQuery(c)
	if problem != nil {
		util.GinProblemJson(c, problem)
		return
	}
	processor, ok := s.processor.(nfDiscoveryProcessor)
	if !ok {
		util.GinProblemJson(c, nfDiscoveryUnavailableProblem())
		return
	}
	result, err := processor.HandleNFDiscovery(c.Request.Context(), query)
	if err == nil && result != nil {
		c.Header("Cache-Control", fmt.Sprintf("max-age=%d", result.ValidityPeriod))
		c.JSON(http.StatusOK, result)
		return
	}
	if errors.Is(err, mtlfprocessor.ErrNFDiscoveryUnavailable) || result == nil && err == nil {
		util.GinProblemJson(c, nfDiscoveryUnavailableProblem())
		return
	}
	var statusError interface{ HTTPStatusCode() int }
	var redirectError interface{ RedirectLocation() string }
	if errors.As(err, &statusError) {
		statusCode := statusError.HTTPStatusCode()
		if (statusCode == http.StatusTemporaryRedirect || statusCode == http.StatusPermanentRedirect) &&
			errors.As(err, &redirectError) && redirectError.RedirectLocation() != "" {
			c.Header("Location", redirectError.RedirectLocation())
			c.Status(statusCode)
			return
		}
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
		Detail: "NRF NF discovery failed",
	})
}

func validateNFDiscoveryQuery(c *gin.Context) (consumer.NFDiscoveryQuery, *models.ProblemDetails) {
	targetType := c.Query("target-nf-type")
	requesterType := c.Query("requester-nf-type")
	serviceNames := strings.Split(c.Query("service-names"), ",")
	acceptedService := ""
	for _, serviceName := range serviceNames {
		name := strings.TrimSpace(serviceName)
		if name == string(models.ServiceName_NSMF_EVENT_EXPOSURE) ||
			name == "nadrf-datamanagement" {
			acceptedService = name
		}
	}
	targetAccepted := targetType == string(models.NrfNfManagementNfType_SMF) ||
		targetType == "ADRF"
	if targetAccepted && requesterType == string(models.NrfNfManagementNfType_NWDAF) &&
		acceptedService != "" &&
		(targetType != "SMF" || acceptedService == "nsmf-event-exposure") &&
		(targetType != "ADRF" || acceptedService == "nadrf-datamanagement") {
		return consumer.NFDiscoveryQuery{
			TargetNFType:    models.NrfNfManagementNfType(targetType),
			RequesterNFType: models.NrfNfManagementNfType(requesterType),
			ServiceNames:    []models.ServiceName{models.ServiceName(acceptedService)},
		}, nil
	}
	return consumer.NFDiscoveryQuery{}, &models.ProblemDetails{
		Status: http.StatusBadRequest,
		Title:  http.StatusText(http.StatusBadRequest),
		Cause:  "MANDATORY_QUERY_PARAM_INCORRECT",
		Detail: "a supported target NF type and matching service name with requester-nf-type=NWDAF are required",
	}
}

func nfDiscoveryUnavailableProblem() *models.ProblemDetails {
	return &models.ProblemDetails{
		Status: http.StatusServiceUnavailable,
		Title:  http.StatusText(http.StatusServiceUnavailable),
		Detail: "NRF NF discovery is temporarily unavailable",
	}
}
