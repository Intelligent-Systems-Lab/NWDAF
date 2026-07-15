package sbi

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/nwdaf/internal/util"
	"github.com/free5gc/openapi/models"
)

type authorizationContext interface {
	AuthorizationCheck(token string, serviceName models.ServiceName) error
}

type routerAuthorizationCheck struct {
	serviceName models.ServiceName
}

func newRouterAuthorizationCheck(serviceName models.ServiceName) *routerAuthorizationCheck {
	return &routerAuthorizationCheck{serviceName: serviceName}
}

func (check *routerAuthorizationCheck) Check(c *gin.Context, authContext authorizationContext) {
	token := c.GetHeader("Authorization")
	if err := authContext.AuthorizationCheck(token, check.serviceName); err != nil {
		logger.SBILog.Debugf(
			"OAuth authorization failed: serviceName=%s cause=%v",
			check.serviceName,
			err,
		)
		util.GinProblemJson(c, &models.ProblemDetails{
			Title:  "Unauthorized",
			Status: http.StatusUnauthorized,
			Detail: "OAuth authorization failed",
		})
		c.Abort()
	}
}
