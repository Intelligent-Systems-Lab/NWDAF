package anlf

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sort"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	anlfprocessor "github.com/free5gc/nwdaf/internal/anlf/processor"
	"github.com/free5gc/nwdaf/internal/backend"
	"github.com/free5gc/nwdaf/internal/util"
	"github.com/free5gc/openapi/models"
)

const maxSmfResourceAssociationBodyBytes = 1024 * 1024

type smfResourceAssociationProcessor interface {
	ReplaceSmfResourceAssociations(backend.SmfResourceAssociationUpdate) error
}

func (s *Server) smfResourceAssociationRoutes() []Route {
	return []Route{{
		Name:    "ReplaceSmfResourceAssociations",
		Method:  http.MethodPut,
		Pattern: "/internal/v1/sync/anlf/smf-resource-associations",
		APIFunc: s.ReplaceSmfResourceAssociations,
	}}
}

func (s *Server) ReplaceSmfResourceAssociations(c *gin.Context) {
	update, problem := readSmfResourceAssociationUpdate(c)
	if problem != nil {
		util.GinProblemJson(c, problem)
		return
	}
	processor, ok := s.processor.(smfResourceAssociationProcessor)
	if !ok {
		util.GinProblemJson(c, smfAssociationProblem(
			http.StatusServiceUnavailable,
			"BACKEND_UNAVAILABLE",
			"AnLF backend association mirror is unavailable",
		))
		return
	}
	err := processor.ReplaceSmfResourceAssociations(update)
	switch {
	case err == nil:
		c.Status(http.StatusNoContent)
	case errors.Is(err, anlfprocessor.ErrBackendUnavailable):
		util.GinProblemJson(c, smfAssociationProblem(
			http.StatusServiceUnavailable,
			"BACKEND_UNAVAILABLE",
			"AnLF backend is not currently usable",
		))
	case errors.Is(err, anlfprocessor.ErrStaleBackendProcess):
		util.GinProblemJson(c, smfAssociationProblem(
			http.StatusConflict,
			"STALE_BACKEND_PROCESS",
			"association update belongs to a stale AnLF backend process",
		))
	case errors.Is(err, anlfprocessor.ErrUnknownSmfResource):
		util.GinProblemJson(c, smfAssociationProblem(
			http.StatusConflict,
			"UNKNOWN_SMF_RESOURCE",
			"association update references an unknown SMF peer resource",
		))
	default:
		util.GinProblemJson(c, smfAssociationProblem(
			http.StatusInternalServerError,
			"SYSTEM_FAILURE",
			"could not replace the SMF resource association mirror",
		))
	}
}

func readSmfResourceAssociationUpdate(
	c *gin.Context,
) (backend.SmfResourceAssociationUpdate, *models.ProblemDetails) {
	var update backend.SmfResourceAssociationUpdate
	c.Request.Body = http.MaxBytesReader(
		c.Writer,
		c.Request.Body,
		maxSmfResourceAssociationBodyBytes,
	)
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&update); err != nil {
		var maxBytesError *http.MaxBytesError
		if errors.As(err, &maxBytesError) {
			return update, smfAssociationProblem(
				http.StatusRequestEntityTooLarge,
				"REQUEST_TOO_LARGE",
				"association snapshot exceeds the configured transport limit",
			)
		}
		return update, malformedSmfAssociationProblem(err.Error())
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return update, malformedSmfAssociationProblem("request contains trailing JSON data")
	}
	if _, err := uuid.Parse(update.ProcessInstanceID); err != nil {
		return update, malformedSmfAssociationProblem("processInstanceId must be a UUID")
	}
	if update.SmfResources == nil {
		return update, malformedSmfAssociationProblem("smfResources is required")
	}

	seenResources := make(map[string]struct{}, len(update.SmfResources))
	for index := range update.SmfResources {
		association := &update.SmfResources[index]
		association.TargetAPIBaseURI = strings.TrimRight(
			strings.TrimSpace(association.TargetAPIBaseURI),
			"/",
		)
		association.PeerSubscriptionID = strings.TrimSpace(association.PeerSubscriptionID)
		if !validTargetAPIBaseURI(association.TargetAPIBaseURI) || association.PeerSubscriptionID == "" {
			return update, malformedSmfAssociationProblem(
				"each SMF resource requires an absolute targetApiRoot and peerSubscriptionId",
			)
		}
		resourceKey := association.TargetAPIBaseURI + "\x00" + association.PeerSubscriptionID
		if _, duplicate := seenResources[resourceKey]; duplicate {
			return update, malformedSmfAssociationProblem("smfResources contains a duplicate peer tuple")
		}
		seenResources[resourceKey] = struct{}{}

		seenSubscriptions := make(map[string]struct{}, len(association.NwdafSubscriptionIDs))
		for idIndex, subscriptionID := range association.NwdafSubscriptionIDs {
			subscriptionID = strings.TrimSpace(subscriptionID)
			if _, err := uuid.Parse(subscriptionID); err != nil {
				return update, malformedSmfAssociationProblem(
					"nwdafSubscriptionIds must contain UUID values",
				)
			}
			if _, duplicate := seenSubscriptions[subscriptionID]; duplicate {
				return update, malformedSmfAssociationProblem(
					"nwdafSubscriptionIds contains a duplicate value",
				)
			}
			seenSubscriptions[subscriptionID] = struct{}{}
			association.NwdafSubscriptionIDs[idIndex] = subscriptionID
		}
		sort.Strings(association.NwdafSubscriptionIDs)
	}
	return update, nil
}

func malformedSmfAssociationProblem(detail string) *models.ProblemDetails {
	return smfAssociationProblem(http.StatusBadRequest, "MALFORMED_REQUEST", detail)
}

func smfAssociationProblem(statusCode int, cause, detail string) *models.ProblemDetails {
	return &models.ProblemDetails{
		Status: int32(statusCode),
		Title:  http.StatusText(statusCode),
		Cause:  cause,
		Detail: detail,
	}
}
