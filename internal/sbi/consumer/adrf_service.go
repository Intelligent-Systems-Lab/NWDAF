package consumer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	adrfcompat "github.com/free5gc/nwdaf/internal/compat/adrf"
	"github.com/free5gc/openapi/models"
)

const maxAdrfStandardBodyBytes = 4 * 1024 * 1024

type StandardAdrfResponse struct {
	StatusCode  int
	Location    string
	ContentType string
	Body        []byte
}

type StandardAdrfError struct {
	StatusCode     int
	ProblemDetails models.ProblemDetails
}

func (e *StandardAdrfError) Error() string {
	return fmt.Sprintf("ADRF Data Management request failed: status=%d", e.StatusCode)
}

func (e *StandardAdrfError) HTTPStatusCode() int {
	return e.StatusCode
}

func (e *StandardAdrfError) StandardProblemDetails() *models.ProblemDetails {
	problem := e.ProblemDetails
	if problem.Status == 0 {
		problem.Status = int32(e.StatusCode)
	}
	return &problem
}

const (
	AdrfDataStoreRecordsPath           = "/nadrf-datamanagement/v1/data-store-records"
	AdrfDataRetrievalSubscriptionsPath = "/nadrf-datamanagement/v1/data-retrieval-subscriptions"
	AdrfMLModelStoreRecordsPath        = "/nadrf-mlmodelmanagement/v1/mlmodel-store-records"
	adrfStorageTimeout                 = 120 * time.Second
	adrfRetrievalTimeout               = 120 * time.Second
)

type (
	AdrfTimePeriod                 = adrfcompat.TimePeriod
	NadrfDataRetrievalSubscription = adrfcompat.DataRetrievalSubscription
	AdrfDataSubscription           = adrfcompat.DataSubscription
	AdrfDataNotification           = adrfcompat.DataNotification
	NadrfDataStoreRecord           = adrfcompat.DataStoreRecord
	NadrfMLModelStoreRecord        = adrfcompat.MLModelStoreRecord
)

// AdrfClient is the HTTP client for the ADRF Data Management and ML Model
// Management services defined by TS 29.575.
type AdrfClient struct {
	endpoint   string
	httpClient *http.Client
}

// NewAdrfClient creates an AdrfClient for the given ADRF endpoint (e.g. "http://127.0.0.1:9888").
func NewAdrfClient(endpoint string) *AdrfClient {
	return &AdrfClient{
		endpoint: endpoint,
		httpClient: &http.Client{
			Timeout: adrfStorageTimeout,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
			Transport: &http.Transport{
				MaxIdleConns:        50,
				MaxIdleConnsPerHost: 10,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
}

func (c *AdrfClient) ExecuteStandardMLModelStoreRequest(
	ctx context.Context,
	body []byte,
) (*StandardAdrfResponse, error) {
	response, err := c.executeStandardMLModelRequest(
		ctx,
		http.MethodPost,
		strings.TrimRight(c.endpoint, "/")+AdrfMLModelStoreRecordsPath,
		body,
	)
	if err != nil {
		return response, err
	}
	if response.StatusCode != http.StatusCreated {
		return response, unexpectedAdrfStatus(response)
	}
	if response.Location == "" || !isJSONMediaType(response.ContentType) ||
		len(response.Body) == 0 {
		return nil, fmt.Errorf("malformed ADRF ML model store response")
	}
	var record NadrfMLModelStoreRecord
	if unmarshalErr := json.Unmarshal(response.Body, &record); unmarshalErr != nil ||
		!validMLModelStoreRecord(record, true) {
		return nil, fmt.Errorf("malformed ADRF ML model store representation")
	}
	return response, nil
}

func (c *AdrfClient) ExecuteStandardMLModelRetrievalRequest(
	ctx context.Context,
	storeTransID string,
	modelUniqueIDs []int64,
) (*StandardAdrfResponse, error) {
	if (strings.TrimSpace(storeTransID) == "") == (len(modelUniqueIDs) == 0) {
		return nil, fmt.Errorf(
			"exactly one of store-trans-id or model-unique-ids is required",
		)
	}
	values := url.Values{}
	if storeTransID != "" {
		values.Set("store-trans-id", storeTransID)
	} else {
		for _, modelID := range modelUniqueIDs {
			if modelID < 0 {
				return nil, fmt.Errorf("model-unique-ids must be non-negative")
			}
			values.Add("model-unique-ids", fmt.Sprintf("%d", modelID))
		}
	}
	requestURL := strings.TrimRight(c.endpoint, "/") + AdrfMLModelStoreRecordsPath +
		"?" + values.Encode()
	response, err := c.executeStandardMLModelRequest(
		ctx,
		http.MethodGet,
		requestURL,
		nil,
	)
	if err != nil {
		return response, err
	}
	switch response.StatusCode {
	case http.StatusOK:
		if !isJSONMediaType(response.ContentType) || len(response.Body) == 0 {
			return nil, fmt.Errorf("malformed ADRF ML model retrieval response")
		}
		var record NadrfMLModelStoreRecord
		if unmarshalErr := json.Unmarshal(response.Body, &record); unmarshalErr != nil ||
			!validMLModelStoreRecord(record, false) {
			return nil, fmt.Errorf("malformed ADRF ML model retrieval representation")
		}
	case http.StatusNoContent:
		if len(response.Body) != 0 {
			return nil, fmt.Errorf("malformed ADRF ML model no-content response")
		}
	case http.StatusTemporaryRedirect, http.StatusPermanentRedirect:
		if response.Location == "" {
			return nil, fmt.Errorf("malformed ADRF ML model redirect response")
		}
	default:
		return response, unexpectedAdrfStatus(response)
	}
	return response, nil
}

func (c *AdrfClient) executeStandardMLModelRequest(
	ctx context.Context,
	method string,
	requestURL string,
	body []byte,
) (*StandardAdrfResponse, error) {
	requestCtx, cancel, err := timeoutContextFromParent(
		ctx,
		adrfStorageTimeout,
		"ADRF ML model request",
	)
	if err != nil {
		return nil, err
	}
	defer cancel()
	request, err := http.NewRequestWithContext(
		requestCtx,
		method,
		requestURL,
		bytes.NewReader(body),
	)
	if err != nil {
		return nil, fmt.Errorf("build ADRF ML model request: %w", err)
	}
	if len(body) > 0 {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := c.httpClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("send ADRF ML model request: %w", err)
	}
	responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, maxAdrfStandardBodyBytes+1))
	closeErr := response.Body.Close()
	if readErr != nil {
		return nil, fmt.Errorf("read ADRF ML model response: %w", readErr)
	}
	if len(responseBody) > maxAdrfStandardBodyBytes {
		return nil, fmt.Errorf("ADRF ML model response exceeds transport limit")
	}
	if closeErr != nil {
		consumerLog.Debugf("failed to close ADRF ML model response: %v", closeErr)
	}
	return &StandardAdrfResponse{
		StatusCode:  response.StatusCode,
		Location:    response.Header.Get("Location"),
		ContentType: response.Header.Get("Content-Type"),
		Body:        responseBody,
	}, nil
}

func unexpectedAdrfStatus(response *StandardAdrfResponse) error {
	problem := models.ProblemDetails{
		Status: int32(response.StatusCode),
		Title:  http.StatusText(response.StatusCode),
	}
	if err := json.Unmarshal(response.Body, &problem); err != nil {
		problem.Detail = strings.TrimSpace(string(response.Body))
	}
	return &StandardAdrfError{
		StatusCode:     response.StatusCode,
		ProblemDetails: problem,
	}
}

func validMLModelStoreRecord(record NadrfMLModelStoreRecord, requireResult bool) bool {
	if (record.NFInstanceID == "") == (record.NFSetID == "") ||
		len(record.MLModelInfo) != 1 {
		return false
	}
	info := record.MLModelInfo[0]
	if info.ModelUniqueID == nil || *info.ModelUniqueID < 0 ||
		info.MLStorageSize == nil || *info.MLStorageSize < 0 ||
		(info.MLFileAddr.MLModelURL == "") == (info.MLFileAddr.MLFileFQDN == "") {
		return false
	}
	if requireResult {
		return record.ModelStoreResult != nil &&
			record.ModelStoreResult.ModelUniqueID != nil &&
			*record.ModelStoreResult.ModelUniqueID == *info.ModelUniqueID &&
			record.ModelStoreResult.StoreResult == "ML_MODEL_FILE_STORED_IN_ADRF"
	}
	return true
}

func (c *AdrfClient) ExecuteStandardStorageRequest(
	ctx context.Context,
	body []byte,
) (*StandardAdrfResponse, error) {
	requestCtx, cancel, err := timeoutContextFromParent(ctx, adrfStorageTimeout, "ADRF storage request")
	if err != nil {
		return nil, err
	}
	defer cancel()
	requestURL := strings.TrimRight(c.endpoint, "/") + AdrfDataStoreRecordsPath
	request, err := http.NewRequestWithContext(
		requestCtx,
		http.MethodPost,
		requestURL,
		bytes.NewReader(body),
	)
	if err != nil {
		return nil, fmt.Errorf("build ADRF storage request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := c.httpClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("send ADRF storage request: %w", err)
	}
	responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, maxAdrfStandardBodyBytes+1))
	closeErr := response.Body.Close()
	if readErr != nil {
		return nil, fmt.Errorf("read ADRF storage response: %w", readErr)
	}
	if len(responseBody) > maxAdrfStandardBodyBytes {
		return nil, fmt.Errorf("ADRF storage response exceeds transport limit")
	}
	if closeErr != nil {
		consumerLog.Debugf("failed to close ADRF storage response: %v", closeErr)
	}
	standardResponse := &StandardAdrfResponse{
		StatusCode:  response.StatusCode,
		Location:    response.Header.Get("Location"),
		ContentType: response.Header.Get("Content-Type"),
		Body:        responseBody,
	}
	if response.StatusCode != http.StatusCreated {
		problem := models.ProblemDetails{
			Status: int32(response.StatusCode),
			Title:  http.StatusText(response.StatusCode),
		}
		if unmarshalErr := json.Unmarshal(responseBody, &problem); unmarshalErr != nil {
			problem.Detail = strings.TrimSpace(string(responseBody))
		}
		return standardResponse, &StandardAdrfError{
			StatusCode:     response.StatusCode,
			ProblemDetails: problem,
		}
	}
	if standardResponse.Location == "" {
		return nil, fmt.Errorf("malformed ADRF storage response: Location is required")
	}
	if !isJSONMediaType(standardResponse.ContentType) {
		return nil, fmt.Errorf("malformed ADRF storage response: Content-Type must be application/json")
	}
	if len(standardResponse.Body) == 0 {
		return nil, fmt.Errorf("malformed ADRF storage response: representation is required")
	}
	var record NadrfDataStoreRecord
	if decodeErr := json.Unmarshal(standardResponse.Body, &record); decodeErr != nil {
		return nil, fmt.Errorf("decode ADRF storage representation: %w", decodeErr)
	}
	if len(record.DataSub) == 0 || record.DataNotif == nil ||
		(len(record.DataNotif.UpfEventNotifs) == 0 && len(record.DataNotif.SmfEventNotifs) == 0) {
		return nil, fmt.Errorf("malformed ADRF storage response: dataSub and a supported dataNotif array are required")
	}
	return standardResponse, nil
}

func (c *AdrfClient) ExecuteStandardRetrievalSubscribe(
	ctx context.Context,
	body []byte,
) (*StandardAdrfResponse, error) {
	return c.executeStandardAdrfRequest(
		ctx,
		http.MethodPost,
		strings.TrimRight(c.endpoint, "/")+AdrfDataRetrievalSubscriptionsPath,
		body,
		http.StatusCreated,
		true,
	)
}

func (c *AdrfClient) ExecuteStandardRetrievalUnsubscribe(
	ctx context.Context,
	resourceLocation string,
) (*StandardAdrfResponse, error) {
	requestURL := strings.TrimSpace(resourceLocation)
	if parsed, err := url.Parse(requestURL); err != nil || !parsed.IsAbs() {
		requestURL = strings.TrimRight(c.endpoint, "/") + "/" + strings.TrimLeft(requestURL, "/")
	}
	return c.executeStandardAdrfRequest(
		ctx,
		http.MethodDelete,
		requestURL,
		nil,
		http.StatusNoContent,
		false,
	)
}

func (c *AdrfClient) executeStandardAdrfRequest(
	ctx context.Context,
	method string,
	requestURL string,
	body []byte,
	successStatus int,
	requireLocation bool,
) (*StandardAdrfResponse, error) {
	requestCtx, cancel, err := timeoutContextFromParent(
		ctx, adrfRetrievalTimeout, "ADRF retrieval control request",
	)
	if err != nil {
		return nil, err
	}
	defer cancel()
	request, err := http.NewRequestWithContext(requestCtx, method, requestURL, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build ADRF retrieval control request: %w", err)
	}
	if len(body) > 0 {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := c.httpClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("send ADRF retrieval control request: %w", err)
	}
	responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, maxAdrfStandardBodyBytes+1))
	closeErr := response.Body.Close()
	if readErr != nil || len(responseBody) > maxAdrfStandardBodyBytes {
		return nil, fmt.Errorf("read ADRF retrieval control response")
	}
	if closeErr != nil {
		consumerLog.Debugf("failed to close ADRF retrieval control response: %v", closeErr)
	}
	result := &StandardAdrfResponse{
		StatusCode:  response.StatusCode,
		Location:    response.Header.Get("Location"),
		ContentType: response.Header.Get("Content-Type"),
		Body:        responseBody,
	}
	if response.StatusCode != successStatus {
		problem := models.ProblemDetails{Status: int32(response.StatusCode), Title: http.StatusText(response.StatusCode)}
		if decodeErr := json.Unmarshal(responseBody, &problem); decodeErr != nil {
			problem.Detail = strings.TrimSpace(string(responseBody))
		}
		return result, &StandardAdrfError{StatusCode: response.StatusCode, ProblemDetails: problem}
	}
	if requireLocation {
		if result.Location == "" || !isJSONMediaType(result.ContentType) || len(result.Body) == 0 {
			return nil, fmt.Errorf("malformed ADRF retrieval create response")
		}
		var representation NadrfDataRetrievalSubscription
		if decodeErr := json.Unmarshal(result.Body, &representation); decodeErr != nil ||
			representation.NotifCorrId == "" || representation.NotificationURI == "" ||
			representation.DataSub.SmfDataSub == nil ||
			representation.TimePeriod.StartTime == "" || representation.TimePeriod.StopTime == "" {
			return nil, fmt.Errorf("malformed ADRF retrieval create representation")
		}
	}
	return result, nil
}

func isJSONMediaType(value string) bool {
	mediaType := strings.ToLower(strings.TrimSpace(strings.SplitN(value, ";", 2)[0]))
	return mediaType == "application/json"
}

func (c *AdrfClient) HTTPClient() *http.Client {
	return c.httpClient
}
