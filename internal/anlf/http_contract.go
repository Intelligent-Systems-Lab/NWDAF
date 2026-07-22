package anlf

import (
	"errors"
	"io"
	"mime"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/free5gc/openapi"
	"github.com/free5gc/openapi/models"
)

const standardJSONMediaType = "application/json"

func readStandardJSONBody(
	c *gin.Context,
	maxBytes int64,
	tooLargeDetail string,
) ([]byte, *models.ProblemDetails) {
	mediaType, _, err := mime.ParseMediaType(c.GetHeader("Content-Type"))
	if err != nil || mediaType != standardJSONMediaType {
		return nil, &models.ProblemDetails{
			Status: http.StatusUnsupportedMediaType,
			Title:  http.StatusText(http.StatusUnsupportedMediaType),
			Cause:  "UNSUPPORTED_MEDIA_TYPE",
			Detail: "Content-Type must be application/json",
		}
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxBytes)
	body, err := io.ReadAll(c.Request.Body)
	if err == nil {
		return body, nil
	}
	var maxBytesError *http.MaxBytesError
	if errors.As(err, &maxBytesError) {
		return nil, &models.ProblemDetails{
			Status: http.StatusRequestEntityTooLarge,
			Title:  http.StatusText(http.StatusRequestEntityTooLarge),
			Cause:  "REQUEST_TOO_LARGE",
			Detail: tooLargeDetail,
		}
	}
	return nil, openapi.ProblemDetailsSystemFailure(err.Error())
}

func malformedRequestProblem(detail string) *models.ProblemDetails {
	return openapi.ProblemDetailsMalformedReqSyntax(detail)
}
