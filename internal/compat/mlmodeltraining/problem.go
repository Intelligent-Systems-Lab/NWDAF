package mlmodeltraining

import (
	"errors"
	"net/http"

	"github.com/free5gc/openapi/models"
)

func ProblemDetailsForValidation(err error) (*models.ProblemDetails, bool) {
	if err == nil {
		return nil, false
	}
	status := 0
	cause := ""
	violations := []InvalidParameter(nil)
	var invalid *InvalidMessageError
	if errors.As(err, &invalid) {
		status = http.StatusBadRequest
		cause = "INVALID_MSG_FORMAT"
		violations = invalid.Violations
	}
	var requirements *RequirementsError
	if status == 0 && errors.As(err, &requirements) {
		status = http.StatusForbidden
		cause = CauseMLModelTrainingRequirementsNotMet
		violations = requirements.Violations
	}
	if status == 0 {
		return nil, false
	}
	invalidParams := make([]models.InvalidParam, 0, len(violations))
	for _, violation := range violations {
		invalidParams = append(invalidParams, models.InvalidParam{
			Param:  violation.Parameter,
			Reason: violation.Reason,
		})
	}
	return &models.ProblemDetails{
		Status:        int32(status),
		Title:         http.StatusText(status),
		Cause:         cause,
		Detail:        err.Error(),
		InvalidParams: invalidParams,
	}, true
}
