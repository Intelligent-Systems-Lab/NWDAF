package sbi

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/nwdaf/pkg/factory"
	"github.com/free5gc/openapi/models"
)

// HandleCreateSubscription handles POST /subscriptions
func (s *Server) HandleCreateSubscription(c *gin.Context) {
	logger.SBILog.Info("Handle CreateSubscription")

	var req models.NnwdafEventsSubscription
	if err := c.ShouldBindJSON(&req); err != nil {
		problemDetails := models.ProblemDetails{
			Status: http.StatusBadRequest,
			Cause:  "INVALID_JSON",
			Detail: err.Error(),
		}
		c.JSON(http.StatusBadRequest, problemDetails)
		return
	}

	response, subscriptionId, problemDetails := s.Processor().HandleCreateSubscription(&req)
	if problemDetails != nil {
		c.JSON(int(problemDetails.Status), problemDetails)
		return
	}

	// Set Location header
	cfg := s.Config()
	locationUri := fmt.Sprintf("%s://%s:%d%s/subscriptions/%s",
		cfg.Configuration.Sbi.Scheme,
		cfg.Configuration.Sbi.RegisterIPv4,
		cfg.Configuration.Sbi.Port,
		factory.NwdafEventsSubResUriPrefix,
		subscriptionId)

	c.Header("Location", locationUri)
	c.JSON(http.StatusCreated, response)
}

// HandleUpdateSubscription handles PUT /subscriptions/:subscriptionId
func (s *Server) HandleUpdateSubscription(c *gin.Context) {
	subscriptionId := c.Param("subscriptionId")
	logger.SBILog.Infof("Handle UpdateSubscription: %s", subscriptionId)

	var req models.NnwdafEventsSubscription
	if err := c.ShouldBindJSON(&req); err != nil {
		problemDetails := models.ProblemDetails{
			Status: http.StatusBadRequest,
			Cause:  "INVALID_JSON",
			Detail: err.Error(),
		}
		c.JSON(http.StatusBadRequest, problemDetails)
		return
	}

	response, problemDetails := s.Processor().HandleUpdateSubscription(subscriptionId, &req)
	if problemDetails != nil {
		c.JSON(int(problemDetails.Status), problemDetails)
		return
	}

	c.JSON(http.StatusOK, response)
}

// HandleDeleteSubscription handles DELETE /subscriptions/:subscriptionId
func (s *Server) HandleDeleteSubscription(c *gin.Context) {
	subscriptionId := c.Param("subscriptionId")
	logger.SBILog.Infof("Handle DeleteSubscription: %s", subscriptionId)

	problemDetails := s.Processor().HandleDeleteSubscription(subscriptionId)
	if problemDetails != nil {
		c.JSON(int(problemDetails.Status), problemDetails)
		return
	}

	c.Status(http.StatusNoContent)
}
