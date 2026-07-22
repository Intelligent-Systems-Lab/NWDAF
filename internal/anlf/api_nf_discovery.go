package anlf

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	anlfprocessor "github.com/free5gc/nwdaf/internal/anlf/processor"
	"github.com/free5gc/nwdaf/internal/util"
	"github.com/free5gc/openapi/models"
)

type nfDiscoveryProcessor interface {
	HandleSmfNFDiscovery(context.Context) (*models.SearchResult, error)
}

func (s *Server) nfDiscoveryRoutes() []Route {
	return []Route{{
		Name:    "HandleSmfNFDiscovery",
		Method:  http.MethodGet,
		Pattern: "/internal/v1/nrf/nf-instances",
		APIFunc: s.HandleSmfNFDiscovery,
	}}
}

func (s *Server) HandleSmfNFDiscovery(c *gin.Context) {
	if problem := validateSmfDiscoveryQuery(c); problem != nil {
		util.GinProblemJson(c, problem)
		return
	}
	processor, ok := s.processor.(nfDiscoveryProcessor)
	if !ok {
		util.GinProblemJson(c, nfDiscoveryUnavailableProblem())
		return
	}
	result, err := processor.HandleSmfNFDiscovery(c.Request.Context())
	if err == nil && result != nil {
		c.JSON(http.StatusOK, result)
		return
	}
	if errors.Is(err, anlfprocessor.ErrNFDiscoveryUnavailable) || result == nil && err == nil {
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

func validateSmfDiscoveryQuery(c *gin.Context) *models.ProblemDetails {
	targetType := c.Query("target-nf-type")
	requesterType := c.Query("requester-nf-type")
	serviceNames := strings.Split(c.Query("service-names"), ",")
	serviceFound := false
	for _, serviceName := range serviceNames {
		if strings.TrimSpace(serviceName) == string(models.ServiceName_NSMF_EVENT_EXPOSURE) {
			serviceFound = true
			break
		}
	}
	if targetType == string(models.NrfNfManagementNfType_SMF) &&
		requesterType == string(models.NrfNfManagementNfType_NWDAF) && serviceFound {
		return nil
	}
	return &models.ProblemDetails{
		Status: http.StatusBadRequest,
		Title:  http.StatusText(http.StatusBadRequest),
		Cause:  "MANDATORY_QUERY_PARAM_INCORRECT",
		Detail: "target-nf-type=SMF, requester-nf-type=NWDAF and service-names containing nsmf-event-exposure are required",
	}
}

func nfDiscoveryUnavailableProblem() *models.ProblemDetails {
	return &models.ProblemDetails{
		Status: http.StatusServiceUnavailable,
		Title:  http.StatusText(http.StatusServiceUnavailable),
		Detail: "NRF NF discovery is temporarily unavailable",
	}
}
