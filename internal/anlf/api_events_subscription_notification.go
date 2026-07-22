package anlf

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/free5gc/nwdaf/internal/sbi/notifier"
	"github.com/free5gc/nwdaf/internal/util"
	"github.com/free5gc/openapi/models"
)

const maxEventsSubscriptionNotificationBodyBytes = 1024 * 1024

type eventsSubscriptionNotificationProcessor interface {
	HandleEventsSubscriptionNotification(
		[]models.NnwdafEventsSubscriptionNotification,
		[]byte,
	) error
}

func (s *Server) eventsSubscriptionNotificationRoutes() []Route {
	return []Route{{
		Name:    "HandleEventsSubscriptionNotification",
		Method:  http.MethodPost,
		Pattern: "/internal/v1/events-subscription-notifications",
		APIFunc: s.HandleEventsSubscriptionNotification,
	}}
}

func (s *Server) HandleEventsSubscriptionNotification(c *gin.Context) {
	rawBody, problem := readStandardJSONBody(
		c,
		maxEventsSubscriptionNotificationBodyBytes,
		"analytics notification body exceeds the configured transport limit",
	)
	if problem != nil {
		util.GinProblemJson(c, problem)
		return
	}
	var notifications []models.NnwdafEventsSubscriptionNotification
	if deserializeErr := json.Unmarshal(rawBody, &notifications); deserializeErr != nil || len(notifications) == 0 {
		detail := "notification array must contain at least one item"
		if deserializeErr != nil {
			detail = deserializeErr.Error()
		}
		util.GinProblemJson(c, malformedRequestProblem(detail))
		return
	}

	processor, ok := s.processor.(eventsSubscriptionNotificationProcessor)
	if !ok {
		util.GinProblemJson(c, &models.ProblemDetails{
			Status: http.StatusServiceUnavailable,
			Title:  http.StatusText(http.StatusServiceUnavailable),
			Detail: "analytics notification routing is unavailable",
		})
		return
	}
	err := processor.HandleEventsSubscriptionNotification(notifications, rawBody)
	if err == nil {
		c.Status(http.StatusNoContent)
		return
	}
	var standardError interface {
		StandardProblemDetails() *models.ProblemDetails
	}
	var redirectError interface{ RedirectLocation() string }
	if errors.As(err, &redirectError) && redirectError.RedirectLocation() != "" {
		c.Header("Location", redirectError.RedirectLocation())
	}
	switch {
	case errors.As(err, &standardError):
		util.GinProblemJson(c, standardError.StandardProblemDetails())
	case errors.Is(err, notifier.ErrSubscriptionNotFound):
		util.GinProblemJson(c, &models.ProblemDetails{
			Status: http.StatusNotFound,
			Title:  http.StatusText(http.StatusNotFound),
			Cause:  "SUBSCRIPTION_NOT_FOUND",
			Detail: "analytics notification references an unknown subscription",
		})
	case errors.Is(err, notifier.ErrInvalidAnalyticsReport):
		util.GinProblemJson(c, &models.ProblemDetails{
			Status: http.StatusBadRequest,
			Title:  http.StatusText(http.StatusBadRequest),
			Cause:  "INVALID_MSG_FORMAT",
			Detail: "analytics notification does not match its subscription route",
		})
	default:
		util.GinProblemJson(c, &models.ProblemDetails{
			Status: http.StatusBadGateway,
			Title:  http.StatusText(http.StatusBadGateway),
			Detail: "analytics notification delivery failed",
		})
	}
}
