package anlf

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"

	anlfprocessor "github.com/free5gc/nwdaf/internal/anlf/processor"
	"github.com/free5gc/nwdaf/internal/backend"
	"github.com/free5gc/nwdaf/internal/sbi/consumer"
	"github.com/free5gc/nwdaf/internal/util"
	"github.com/free5gc/openapi/models"
)

type nfDiscoveryProcessor interface {
	HandleNFDiscovery(context.Context, backend.NFDiscoveryQuery) (*consumer.NFDiscoveryResult, error)
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

func validateNFDiscoveryQuery(c *gin.Context) (backend.NFDiscoveryQuery, *models.ProblemDetails) {
	return backend.ParseNFDiscoveryQuery(c.Request.URL.Query())
}

func nfDiscoveryUnavailableProblem() *models.ProblemDetails {
	return &models.ProblemDetails{
		Status: http.StatusServiceUnavailable,
		Title:  http.StatusText(http.StatusServiceUnavailable),
		Detail: "NRF NF discovery is temporarily unavailable",
	}
}
