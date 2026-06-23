package util

import (
	"github.com/gin-gonic/gin"

	"github.com/free5gc/openapi/models"
)

const ProblemJSONContentType = "application/problem+json"

func GinProblemJson(c *gin.Context, problemDetails *models.ProblemDetails) {
	c.Writer.Header().Set("Content-Type", ProblemJSONContentType)
	c.JSON(int(problemDetails.Status), problemDetails)
}
