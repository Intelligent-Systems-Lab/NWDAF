package sbi

import (
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/nwdaf/internal/util"
	"github.com/free5gc/nwdaf/pkg/factory"
	"github.com/free5gc/openapi"
	"github.com/free5gc/openapi/models"
)

const maxEventsSubscriptionBodyBytes = 1024 * 1024

// HandleCreateSubscription handles POST /subscriptions
func (s *Server) HandleCreateSubscription(c *gin.Context) {
	logger.SBILog.Info("Handle CreateSubscription")

	req, problem := readEventsSubscriptionBody(c)
	if problem != nil {
		util.GinProblemJson(c, problem)
		return
	}

	response, subscriptionId, problemDetails := s.Processor().HandleCreateSubscription(c.Request.Context(), req)
	if problemDetails != nil {
		util.GinProblemJson(c, problemDetails)
		return
	}

	// Set Location header
	cfg := s.Config()
	locationUri := fmt.Sprintf("%s%s/subscriptions/%s",
		cfg.GetSbiUri(),
		factory.NwdafEventsSubResUriPrefix,
		subscriptionId)

	c.Header("Location", locationUri)
	c.JSON(http.StatusCreated, response)
}

// HandleUpdateSubscription handles PUT /subscriptions/:subscriptionId
func (s *Server) HandleUpdateSubscription(c *gin.Context) {
	subscriptionId := c.Param("subscriptionId")
	logger.SBILog.Infof("Handle UpdateSubscription: sub=%s", subscriptionId)

	req, problem := readEventsSubscriptionBody(c)
	if problem != nil {
		util.GinProblemJson(c, problem)
		return
	}

	response, problemDetails := s.Processor().HandleUpdateSubscription(c.Request.Context(), subscriptionId, req)
	if problemDetails != nil {
		util.GinProblemJson(c, problemDetails)
		return
	}

	c.JSON(http.StatusOK, response)
}

func readEventsSubscriptionBody(c *gin.Context) (*models.NnwdafEventsSubscription, *models.ProblemDetails) {
	mediaType, _, err := mime.ParseMediaType(c.GetHeader("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return nil, &models.ProblemDetails{
			Status: http.StatusUnsupportedMediaType,
			Title:  http.StatusText(http.StatusUnsupportedMediaType),
			Detail: "Content-Type must be application/json",
		}
	}

	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxEventsSubscriptionBodyBytes)
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		var maxBytesError *http.MaxBytesError
		if errors.As(err, &maxBytesError) {
			return nil, &models.ProblemDetails{
				Status: http.StatusRequestEntityTooLarge,
				Title:  http.StatusText(http.StatusRequestEntityTooLarge),
				Detail: "Events Subscription body exceeds the configured transport limit",
			}
		}
		logger.SBILog.Errorf("Read Events Subscription body error: %+v", err)
		return nil, openapi.ProblemDetailsSystemFailure(err.Error())
	}

	var req models.NnwdafEventsSubscription
	if deserializeErr := openapi.Deserialize(&req, body, "application/json"); deserializeErr != nil {
		return nil, openapi.ProblemDetailsMalformedReqSyntax(deserializeErr.Error())
	}
	return &req, nil
}

// HandleDeleteSubscription handles DELETE /subscriptions/:subscriptionId
func (s *Server) HandleDeleteSubscription(c *gin.Context) {
	subscriptionId := c.Param("subscriptionId")
	logger.SBILog.Infof("Handle DeleteSubscription: sub=%s", subscriptionId)

	problemDetails := s.Processor().HandleDeleteSubscription(subscriptionId)
	if problemDetails != nil {
		util.GinProblemJson(c, problemDetails)
		return
	}

	c.Status(http.StatusNoContent)
}
