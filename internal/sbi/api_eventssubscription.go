package sbi

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/nwdaf/internal/util"
	"github.com/free5gc/nwdaf/pkg/factory"
	"github.com/free5gc/openapi"
	"github.com/free5gc/openapi/models"
)

// HandleCreateSubscription handles POST /subscriptions
func (s *Server) HandleCreateSubscription(c *gin.Context) {
	logger.SBILog.Info("Handle CreateSubscription")

	var req models.NnwdafEventsSubscription
	requestBody, err := c.GetRawData()
	if err != nil {
		logger.SBILog.Errorf("Get Request Body error: %+v", err)
		util.GinProblemJson(c, openapi.ProblemDetailsSystemFailure(err.Error()))
		return
	}

	if deserializeErr := openapi.Deserialize(&req, requestBody, "application/json"); deserializeErr != nil {
		util.GinProblemJson(c, openapi.ProblemDetailsMalformedReqSyntax(deserializeErr.Error()))
		return
	}

	response, subscriptionId, problemDetails := s.Processor().HandleCreateSubscription(&req)
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

	var req models.NnwdafEventsSubscription
	requestBody, err := c.GetRawData()
	if err != nil {
		logger.SBILog.Errorf("Get Request Body error: %+v", err)
		util.GinProblemJson(c, openapi.ProblemDetailsSystemFailure(err.Error()))
		return
	}

	if deserializeErr := openapi.Deserialize(&req, requestBody, "application/json"); deserializeErr != nil {
		util.GinProblemJson(c, openapi.ProblemDetailsMalformedReqSyntax(deserializeErr.Error()))
		return
	}

	response, problemDetails := s.Processor().HandleUpdateSubscription(subscriptionId, &req)
	if problemDetails != nil {
		util.GinProblemJson(c, problemDetails)
		return
	}

	c.JSON(http.StatusOK, response)
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
