package mtlf

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	mtlfprocessor "github.com/free5gc/nwdaf/internal/mtlf/processor"
	"github.com/free5gc/nwdaf/internal/sbi/consumer"
	"github.com/free5gc/nwdaf/internal/util"
	"github.com/free5gc/openapi/models"
)

type udmCollectionProcessor interface {
	GetUdmGroupIdentifiers(context.Context, string, string, bool) (*consumer.StandardUdmResponse, error)
	GetUdmSmfRegistration(context.Context, string, string, *models.Snssai, string) (*consumer.StandardUdmResponse, error)
}

func (s *Server) udmCollectionRoutes() []Route {
	return []Route{
		{
			Name:    "GetUdmGroupIdentifiers",
			Method:  http.MethodGet,
			Pattern: "/internal/v1/udm-sdm/group-data/group-identifiers",
			APIFunc: s.GetUdmGroupIdentifiers,
		},
		{
			Name:    "GetUdmSmfRegistration",
			Method:  http.MethodGet,
			Pattern: "/internal/v1/udm-uecm/:ueId/registrations/smf-registrations",
			APIFunc: s.GetUdmSmfRegistration,
		},
	}
}

func (s *Server) GetUdmGroupIdentifiers(c *gin.Context) {
	target, ok := readCollectionTargetAPIBaseURI(c)
	if !ok {
		return
	}
	intGroupID := strings.TrimSpace(c.Query("int-group-id"))
	query := c.Request.URL.Query()
	if intGroupID == "" || c.Query("ue-id-ind") != "true" || len(query) != 2 ||
		len(query["int-group-id"]) != 1 || len(query["ue-id-ind"]) != 1 {
		util.GinProblemJson(c, malformedRequestProblem("int-group-id and ue-id-ind=true are required"))
		return
	}
	processor, ok := s.processor.(udmCollectionProcessor)
	if !ok {
		util.GinProblemJson(c, udmCollectionUnavailableProblem())
		return
	}
	response, err := processor.GetUdmGroupIdentifiers(c.Request.Context(), target, intGroupID, true)
	s.writeUdmCollectionResponse(c, response, err)
}

func (s *Server) GetUdmSmfRegistration(c *gin.Context) {
	target, ok := readCollectionTargetAPIBaseURI(c)
	if !ok {
		return
	}
	ueID := strings.TrimSpace(c.Param("ueId"))
	if ueID == "" {
		util.GinProblemJson(c, malformedRequestProblem("ueId is required"))
		return
	}
	var singleNssai *models.Snssai
	if raw := strings.TrimSpace(c.Query("single-nssai")); raw != "" {
		var value models.Snssai
		if err := json.Unmarshal([]byte(raw), &value); err != nil {
			util.GinProblemJson(c, malformedRequestProblem("single-nssai must be a JSON Snssai"))
			return
		}
		singleNssai = &value
	}
	for name, values := range c.Request.URL.Query() {
		if (name != "single-nssai" && name != "dnn") || len(values) != 1 {
			util.GinProblemJson(c, malformedRequestProblem("unsupported or repeated query parameter"))
			return
		}
	}
	processor, ok := s.processor.(udmCollectionProcessor)
	if !ok {
		util.GinProblemJson(c, udmCollectionUnavailableProblem())
		return
	}
	response, err := processor.GetUdmSmfRegistration(
		c.Request.Context(),
		target,
		ueID,
		singleNssai,
		strings.TrimSpace(c.Query("dnn")),
	)
	s.writeUdmCollectionResponse(c, response, err)
}

func (s *Server) writeUdmCollectionResponse(
	c *gin.Context,
	response *consumer.StandardUdmResponse,
	err error,
) {
	if err == nil && response != nil {
		contentType := response.ContentType
		if contentType == "" {
			contentType = standardJSONMediaType
		}
		c.Data(response.StatusCode, contentType, response.Body)
		return
	}
	if errors.Is(err, mtlfprocessor.ErrUdmCollectionUnavailable) || response == nil && err == nil {
		util.GinProblemJson(c, udmCollectionUnavailableProblem())
		return
	}
	var standardError interface{ StandardProblemDetails() *models.ProblemDetails }
	if errors.As(err, &standardError) {
		util.GinProblemJson(c, standardError.StandardProblemDetails())
		return
	}
	util.GinProblemJson(c, &models.ProblemDetails{
		Status: http.StatusBadGateway,
		Title:  http.StatusText(http.StatusBadGateway),
		Detail: "UDM request failed",
	})
}

func udmCollectionUnavailableProblem() *models.ProblemDetails {
	return &models.ProblemDetails{
		Status: http.StatusServiceUnavailable,
		Title:  http.StatusText(http.StatusServiceUnavailable),
		Detail: "UDM collection transport is temporarily unavailable",
	}
}
