package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/free5gc/openapi/models"
)

const maxUdmStandardBodyBytes = 4 * 1024 * 1024

type StandardUdmResponse struct {
	StatusCode  int
	ContentType string
	Body        []byte
}

type StandardUdmError struct {
	StatusCode     int
	ProblemDetails models.ProblemDetails
}

func (e *StandardUdmError) Error() string {
	return fmt.Sprintf("UDM request failed: status=%d", e.StatusCode)
}

func (e *StandardUdmError) StandardProblemDetails() *models.ProblemDetails {
	problem := e.ProblemDetails
	if problem.Status == 0 {
		problem.Status = int32(e.StatusCode)
	}
	return &problem
}

func (c *Consumer) GetUdmGroupIdentifiers(
	ctx context.Context,
	targetAPIBaseURI string,
	intGroupID string,
	ueIDInd bool,
) (*StandardUdmResponse, error) {
	query := url.Values{}
	query.Set("int-group-id", intGroupID)
	query.Set("ue-id-ind", fmt.Sprintf("%t", ueIDInd))
	return c.executeStandardUdmGet(
		ctx,
		targetAPIBaseURI,
		models.ServiceName_NUDM_SDM,
		"/nudm-sdm/v2/group-data/group-identifiers?"+query.Encode(),
		func(body []byte) error {
			var representation models.UdmSdmGroupIdentifiers
			return json.Unmarshal(body, &representation)
		},
	)
}

func (c *Consumer) GetUdmSmfRegistration(
	ctx context.Context,
	targetAPIBaseURI string,
	ueID string,
	singleNssai *models.Snssai,
	dnn string,
) (*StandardUdmResponse, error) {
	query := url.Values{}
	if singleNssai != nil {
		encoded, err := json.Marshal(singleNssai)
		if err != nil {
			return nil, fmt.Errorf("encode single-nssai: %w", err)
		}
		query.Set("single-nssai", string(encoded))
	}
	if dnn != "" {
		query.Set("dnn", dnn)
	}
	requestPath := "/nudm-uecm/v1/" + url.PathEscape(ueID) + "/registrations/smf-registrations"
	if encoded := query.Encode(); encoded != "" {
		requestPath += "?" + encoded
	}
	return c.executeStandardUdmGet(
		ctx,
		targetAPIBaseURI,
		models.ServiceName_NUDM_UECM,
		requestPath,
		func(body []byte) error {
			var representation models.SmfRegistrationInfo
			return json.Unmarshal(body, &representation)
		},
	)
}

func (c *Consumer) executeStandardUdmGet(
	ctx context.Context,
	targetAPIBaseURI string,
	serviceName models.ServiceName,
	requestPath string,
	validateSuccess func([]byte) error,
) (*StandardUdmResponse, error) {
	baseURI, err := validateTargetAPIBaseURI(targetAPIBaseURI)
	if err != nil {
		return nil, err
	}
	requestCtx, err := c.udmRequestContext(ctx, serviceName)
	if err != nil {
		return nil, err
	}
	requestCtx, cancel, err := timeoutContextFromParent(requestCtx, 30*time.Second, "UDM request")
	if err != nil {
		return nil, err
	}
	defer cancel()
	request, err := http.NewRequestWithContext(requestCtx, http.MethodGet, baseURI+requestPath, nil)
	if err != nil {
		return nil, fmt.Errorf("create UDM request: %w", err)
	}
	request.Header.Set("Accept", "application/json, application/problem+json")
	if err = bindOAuthTokenToRequest(request, requestCtx); err != nil {
		return nil, fmt.Errorf("bind OAuth2 token to UDM request: %w", err)
	}
	client := *c.mlModelPeerHTTPClient
	client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("send UDM request: %w", err)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxUdmStandardBodyBytes+1))
	if err != nil {
		if closeErr := response.Body.Close(); closeErr != nil {
			return nil, fmt.Errorf(
				"read UDM response: %w; close response body: %v",
				err,
				closeErr,
			)
		}
		return nil, fmt.Errorf("read UDM response: %w", err)
	}
	if err = response.Body.Close(); err != nil {
		return nil, fmt.Errorf("close UDM response: %w", err)
	}
	if len(body) > maxUdmStandardBodyBytes {
		return nil, errors.New("UDM response exceeds transport limit")
	}
	standard := &StandardUdmResponse{
		StatusCode:  response.StatusCode,
		ContentType: response.Header.Get("Content-Type"),
		Body:        body,
	}
	if response.StatusCode == http.StatusOK {
		if !isJSONMediaType(standard.ContentType) || len(body) == 0 {
			return standard, errors.New("malformed UDM success response")
		}
		if err = validateSuccess(body); err != nil {
			return standard, fmt.Errorf("decode UDM success response: %w", err)
		}
		return standard, nil
	}
	problem := models.ProblemDetails{Status: int32(response.StatusCode), Title: http.StatusText(response.StatusCode)}
	if err = json.Unmarshal(body, &problem); err != nil {
		problem.Detail = strings.TrimSpace(string(body))
	}
	return standard, &StandardUdmError{StatusCode: response.StatusCode, ProblemDetails: problem}
}

func (c *Consumer) udmRequestContext(
	ctx context.Context,
	serviceName models.ServiceName,
) (context.Context, error) {
	nwdafCtx := c.Context()
	if nwdafCtx == nil || !nwdafCtx.RegistrationState().OAuth2Required {
		return ctx, nil
	}
	requestCtx, err := c.nrfService.getTokenContext(
		ctx,
		nwdafCtx,
		serviceName,
		models.NrfNfManagementNfType_UDM,
	)
	if err != nil {
		return nil, fmt.Errorf("authorize UDM request: %w", err)
	}
	return requestCtx, nil
}
