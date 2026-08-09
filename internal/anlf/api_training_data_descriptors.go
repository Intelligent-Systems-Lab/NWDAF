package anlf

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	anlfprocessor "github.com/free5gc/nwdaf/internal/anlf/processor"
	"github.com/free5gc/nwdaf/internal/backend"
	"github.com/free5gc/nwdaf/internal/util"
	"github.com/free5gc/openapi/models"
)

const maxTrainingDataDescriptorBodyBytes = 1024 * 1024

type trainingDataDescriptorProcessor interface {
	PutTrainingDataDescriptor(context.Context, string, []byte) error
	DeleteTrainingDataDescriptor(context.Context, string) error
}

func (s *Server) trainingDataDescriptorRoutes() []Route {
	return []Route{
		{
			Name:    "PutTrainingDataDescriptor",
			Method:  http.MethodPut,
			Pattern: "/internal/v1/anlf/training-data-descriptors/:descriptorId",
			APIFunc: s.PutTrainingDataDescriptor,
		},
		{
			Name:    "DeleteTrainingDataDescriptor",
			Method:  http.MethodDelete,
			Pattern: "/internal/v1/anlf/training-data-descriptors/:descriptorId",
			APIFunc: s.DeleteTrainingDataDescriptor,
		},
	}
}

func (s *Server) PutTrainingDataDescriptor(c *gin.Context) {
	descriptorID := c.Param("descriptorId")
	if uuid.Validate(descriptorID) != nil {
		util.GinProblemJson(c, malformedRequestProblem("descriptorId must be a UUID"))
		return
	}
	body, problem := readStandardJSONBody(
		c,
		maxTrainingDataDescriptorBodyBytes,
		"training-data descriptor exceeds the configured transport limit",
	)
	if problem != nil {
		util.GinProblemJson(c, problem)
		return
	}
	var descriptor backend.TrainingDataDescriptor
	if err := json.Unmarshal(body, &descriptor); err != nil {
		util.GinProblemJson(c, malformedRequestProblem(err.Error()))
		return
	}
	if descriptor.CorrelationID != descriptorID {
		util.GinProblemJson(c, malformedRequestProblem("descriptorId must match correlationId"))
		return
	}
	if validationProblem := validateTrainingDataDescriptor(descriptor); validationProblem != nil {
		util.GinProblemJson(c, validationProblem)
		return
	}
	processor, ok := s.processor.(trainingDataDescriptorProcessor)
	if !ok {
		util.GinProblemJson(c, descriptorRelayUnavailableProblem())
		return
	}
	if err := processor.PutTrainingDataDescriptor(c.Request.Context(), descriptorID, body); err != nil {
		s.writeTrainingDataDescriptorError(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}

func (s *Server) DeleteTrainingDataDescriptor(c *gin.Context) {
	descriptorID := c.Param("descriptorId")
	if uuid.Validate(descriptorID) != nil {
		util.GinProblemJson(c, malformedRequestProblem("descriptorId must be a UUID"))
		return
	}
	processor, ok := s.processor.(trainingDataDescriptorProcessor)
	if !ok {
		util.GinProblemJson(c, descriptorRelayUnavailableProblem())
		return
	}
	if err := processor.DeleteTrainingDataDescriptor(c.Request.Context(), descriptorID); err != nil {
		s.writeTrainingDataDescriptorError(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}

func validateTrainingDataDescriptor(
	descriptor backend.TrainingDataDescriptor,
) *models.ProblemDetails {
	if descriptor.State != "ACTIVE" && descriptor.State != "RETAINED" {
		return malformedRequestProblem("descriptor state must be ACTIVE or RETAINED")
	}
	if descriptor.StoredDataSpec.DataSpec.SmfDataSub == nil ||
		descriptor.StoredDataSpec.TimePeriod.StartTime == nil ||
		descriptor.StoredDataSpec.TimePeriod.StopTime == nil ||
		descriptor.StoredDataSpec.TimePeriod.StartTime.After(
			*descriptor.StoredDataSpec.TimePeriod.StopTime,
		) {
		return malformedRequestProblem("descriptor storedDataSpec is incomplete")
	}
	if descriptor.MLEventSubscription.MLEvent == "" ||
		uuid.Validate(descriptor.SourceNFInstanceID) != nil ||
		(descriptor.ADRFInstanceID != "" && uuid.Validate(descriptor.ADRFInstanceID) != nil) ||
		descriptor.RetainUntil.IsZero() {
		return malformedRequestProblem("descriptor identity and ML event fields are invalid")
	}
	return nil
}

func (s *Server) writeTrainingDataDescriptorError(c *gin.Context, err error) {
	if errors.Is(err, anlfprocessor.ErrTrainingDataDescriptorUnavailable) {
		util.GinProblemJson(c, descriptorRelayUnavailableProblem())
		return
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
		Detail: "training-data descriptor relay failed",
	})
}

func descriptorRelayUnavailableProblem() *models.ProblemDetails {
	return &models.ProblemDetails{
		Status: http.StatusServiceUnavailable,
		Title:  http.StatusText(http.StatusServiceUnavailable),
		Detail: "MTLF backend is temporarily unavailable",
	}
}
