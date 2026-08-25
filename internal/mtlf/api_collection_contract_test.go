package mtlf

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/free5gc/nwdaf/internal/sbi/consumer"
	"github.com/free5gc/nwdaf/internal/util"
	"github.com/free5gc/openapi/models"
)

type collectionContractStub struct {
	target         string
	body           []byte
	ueID           string
	intGroupID     string
	smfResponse    *consumer.StandardSmfResponse
	udmResponse    *consumer.StandardUdmResponse
	adrfResponse   *consumer.StandardAdrfResponse
	smfErr         error
	udmErr         error
	adrfErr        error
	subscriptionID string
	singleNssai    *models.Snssai
	dnn            string
}

func (s *collectionContractStub) CreateSmfEventExposure(
	_ context.Context,
	target string,
	body []byte,
) (*consumer.StandardSmfResponse, error) {
	s.target = target
	s.body = append([]byte(nil), body...)
	return s.smfResponse, s.smfErr
}

func (s *collectionContractStub) ReadSmfEventExposure(
	_ context.Context,
	target string,
	subscriptionID string,
) (*consumer.StandardSmfResponse, error) {
	s.target = target
	s.subscriptionID = subscriptionID
	return s.smfResponse, s.smfErr
}

func (s *collectionContractStub) ReplaceSmfEventExposure(
	_ context.Context,
	target string,
	subscriptionID string,
	body []byte,
) (*consumer.StandardSmfResponse, error) {
	s.target = target
	s.subscriptionID = subscriptionID
	s.body = append([]byte(nil), body...)
	return s.smfResponse, s.smfErr
}

func (s *collectionContractStub) DeleteSmfEventExposure(
	_ context.Context,
	target string,
	subscriptionID string,
) (*consumer.StandardSmfResponse, error) {
	s.target = target
	s.subscriptionID = subscriptionID
	return s.smfResponse, s.smfErr
}

func (s *collectionContractStub) GetUdmGroupIdentifiers(
	_ context.Context,
	target string,
	intGroupID string,
	_ bool,
) (*consumer.StandardUdmResponse, error) {
	s.target = target
	s.intGroupID = intGroupID
	return s.udmResponse, s.udmErr
}

func (s *collectionContractStub) GetUdmSmfRegistration(
	_ context.Context,
	target string,
	ueID string,
	singleNssai *models.Snssai,
	dnn string,
) (*consumer.StandardUdmResponse, error) {
	s.target = target
	s.ueID = ueID
	s.singleNssai = singleNssai
	s.dnn = dnn
	return s.udmResponse, s.udmErr
}

func (s *collectionContractStub) StoreAdrfDataRecord(
	_ context.Context,
	target string,
	body []byte,
) (*consumer.StandardAdrfResponse, error) {
	s.target = target
	s.body = append([]byte(nil), body...)
	return s.adrfResponse, s.adrfErr
}

func TestMTLFCollectionRoutesForwardStandardResponses(t *testing.T) {
	gin.SetMode(gin.TestMode)
	smfBody := `{
		"supi":"imsi-466920000000001",
		"nfId":"11111111-1111-4111-8111-111111111111",
		"notifId":"22222222-2222-4222-8222-222222222222",
		"notifUri":"http://py-mtlf.example/callbacks/upf-event-exposure",
		"eventSubs":[{"event":"UPF_EVENT"}]
	}`
	adrfBody := `{"dataSub":[{"smfDataSub":{"notifId":"corr-a"}}],` +
		`"dataNotif":{"upfEventNotifs":[{"correlationId":"corr-a"}]}}`
	stub := &collectionContractStub{
		smfResponse: &consumer.StandardSmfResponse{
			StatusCode:  http.StatusCreated,
			Location:    "http://smf.example/nsmf-event-exposure/v1/subscriptions/sub-a",
			ContentType: "application/json",
			Body:        []byte(smfBody),
		},
		udmResponse: &consumer.StandardUdmResponse{
			StatusCode:  http.StatusOK,
			ContentType: "application/json",
			Body:        []byte(`{"intGroupId":"group-a","ueIdList":[]}`),
		},
		adrfResponse: &consumer.StandardAdrfResponse{
			StatusCode:  http.StatusCreated,
			Location:    "http://adrf.example/nadrf-datamanagement/v1/data-store-records/record-a",
			ContentType: "application/json",
			Body:        []byte(`{"recordId":"record-a"}`),
		},
	}
	server := &Server{processor: stub}

	t.Run("SMF create", func(t *testing.T) {
		recorder, ctx := collectionContext(t, http.MethodPost, smfBody)
		ctx.Request.Header.Set("Content-Type", "application/json")
		ctx.Request.Header.Set("Target-Api-Root", "http://smf.example")
		server.CreateSmfEventExposure(ctx)
		ctx.Writer.WriteHeaderNow()
		if recorder.Code != http.StatusCreated || recorder.Header().Get("Location") == "" ||
			stub.target != "http://smf.example" || string(stub.body) != smfBody {
			t.Fatalf(
				"status=%d target=%q Location=%q body=%s",
				recorder.Code,
				stub.target,
				recorder.Header().Get("Location"),
				recorder.Body.String(),
			)
		}
	})

	t.Run("SMF read replace delete", func(t *testing.T) {
		operations := []struct {
			name       string
			method     string
			body       string
			statusCode int
			invoke     func(*Server, *gin.Context)
		}{
			{"read", http.MethodGet, "", http.StatusOK, (*Server).ReadSmfEventExposure},
			{"replace", http.MethodPut, smfBody, http.StatusOK, (*Server).ReplaceSmfEventExposure},
			{"delete", http.MethodDelete, "", http.StatusNoContent, (*Server).DeleteSmfEventExposure},
		}
		for _, operation := range operations {
			t.Run(operation.name, func(t *testing.T) {
				stub.smfResponse = &consumer.StandardSmfResponse{
					StatusCode:  operation.statusCode,
					ContentType: "application/json",
					Body:        []byte(smfBody),
				}
				if operation.statusCode == http.StatusNoContent {
					stub.smfResponse.Body = nil
				}
				recorder, ctx := collectionContext(t, operation.method, operation.body)
				ctx.Params = gin.Params{{Key: "subscriptionId", Value: "sub-a"}}
				ctx.Request.Header.Set("Content-Type", "application/json")
				ctx.Request.Header.Set("Target-Api-Root", "http://smf.example")
				operation.invoke(server, ctx)
				ctx.Writer.WriteHeaderNow()
				if recorder.Code != operation.statusCode || stub.subscriptionID != "sub-a" {
					t.Fatalf(
						"status=%d subscriptionID=%q body=%s",
						recorder.Code,
						stub.subscriptionID,
						recorder.Body.String(),
					)
				}
			})
		}
	})

	t.Run("UDM group", func(t *testing.T) {
		recorder, ctx := collectionContext(
			t,
			http.MethodGet,
			"",
			"?int-group-id=group-a&ue-id-ind=true",
		)
		ctx.Request.Header.Set("Target-Api-Root", "http://udm.example")
		server.GetUdmGroupIdentifiers(ctx)
		ctx.Writer.WriteHeaderNow()
		if recorder.Code != http.StatusOK || stub.intGroupID != "group-a" ||
			stub.target != "http://udm.example" {
			t.Fatalf("status=%d target=%q group=%q body=%s", recorder.Code, stub.target, stub.intGroupID, recorder.Body.String())
		}
	})

	t.Run("UDM SMF registration", func(t *testing.T) {
		stub.udmResponse = &consumer.StandardUdmResponse{
			StatusCode:  http.StatusOK,
			ContentType: "application/json",
			Body:        []byte(`{"smfRegistrationList":[]}`),
		}
		recorder, ctx := collectionContext(
			t,
			http.MethodGet,
			"",
			`?single-nssai={"sst":1,"sd":"010203"}&dnn=internet`,
		)
		ctx.Params = gin.Params{{Key: "ueId", Value: "imsi-466920000000001"}}
		ctx.Request.Header.Set("Target-Api-Root", "http://udm.example")
		server.GetUdmSmfRegistration(ctx)
		ctx.Writer.WriteHeaderNow()
		if recorder.Code != http.StatusOK || stub.ueID != "imsi-466920000000001" ||
			stub.singleNssai == nil || stub.singleNssai.Sst != 1 ||
			stub.singleNssai.Sd != "010203" || stub.dnn != "internet" {
			t.Fatalf(
				"status=%d ueID=%q snssai=%+v dnn=%q body=%s",
				recorder.Code,
				stub.ueID,
				stub.singleNssai,
				stub.dnn,
				recorder.Body.String(),
			)
		}
	})

	t.Run("ADRF store", func(t *testing.T) {
		recorder, ctx := collectionContext(t, http.MethodPost, adrfBody)
		ctx.Request.Header.Set("Content-Type", "application/json")
		ctx.Request.Header.Set("Target-Api-Root", "http://adrf.example")
		server.StoreAdrfDataRecord(ctx)
		ctx.Writer.WriteHeaderNow()
		if recorder.Code != http.StatusCreated || recorder.Header().Get("Location") == "" ||
			stub.target != "http://adrf.example" || string(stub.body) != adrfBody {
			t.Fatalf(
				"status=%d target=%q Location=%q body=%s",
				recorder.Code,
				stub.target,
				recorder.Header().Get("Location"),
				recorder.Body.String(),
			)
		}
	})
}

func TestMTLFCollectionRoutesRejectInvalidTargetAndBody(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tests := []struct {
		name   string
		target string
		body   string
		invoke func(*Server, *gin.Context)
	}{
		{
			name:   "target path",
			target: "http://peer.example/unexpected",
			body:   `{}`,
			invoke: func(server *Server, ctx *gin.Context) { server.CreateSmfEventExposure(ctx) },
		},
		{
			name:   "target credentials",
			target: "http://user:password@peer.example",
			body:   `{}`,
			invoke: func(server *Server, ctx *gin.Context) { server.CreateSmfEventExposure(ctx) },
		},
		{
			name:   "target query",
			target: "http://peer.example?target=other",
			body:   `{}`,
			invoke: func(server *Server, ctx *gin.Context) { server.CreateSmfEventExposure(ctx) },
		},
		{
			name:   "target fragment",
			target: "http://peer.example#fragment",
			body:   `{}`,
			invoke: func(server *Server, ctx *gin.Context) { server.CreateSmfEventExposure(ctx) },
		},
		{
			name:   "target scheme",
			target: "ftp://peer.example",
			body:   `{}`,
			invoke: func(server *Server, ctx *gin.Context) { server.CreateSmfEventExposure(ctx) },
		},
		{
			name:   "missing SMF fields",
			target: "http://smf.example",
			body:   `{}`,
			invoke: func(server *Server, ctx *gin.Context) { server.CreateSmfEventExposure(ctx) },
		},
		{
			name:   "missing ADRF notification",
			target: "http://adrf.example",
			body:   `{"dataSub":[{"smfDataSub":{}}],"dataNotif":{}}`,
			invoke: func(server *Server, ctx *gin.Context) { server.StoreAdrfDataRecord(ctx) },
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder, ctx := collectionContext(t, http.MethodPost, test.body)
			ctx.Request.Header.Set("Content-Type", "application/json")
			ctx.Request.Header.Set("Target-Api-Root", test.target)
			test.invoke(&Server{processor: &collectionContractStub{}}, ctx)
			ctx.Writer.WriteHeaderNow()
			if recorder.Code != http.StatusBadRequest ||
				recorder.Header().Get("Content-Type") != util.ProblemJSONContentType {
				t.Fatalf(
					"status=%d Content-Type=%q body=%s",
					recorder.Code,
					recorder.Header().Get("Content-Type"),
					recorder.Body.String(),
				)
			}
		})
	}
}

func TestMTLFCollectionRoutesEnforceBodyBounds(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tests := []struct {
		name   string
		limit  int64
		target string
		invoke func(*Server, *gin.Context)
	}{
		{"SMF", maxSmfEventExposureBodyBytes, "http://smf.example", (*Server).CreateSmfEventExposure},
		{"ADRF", maxAdrfStorageBodyBytes, "http://adrf.example", (*Server).StoreAdrfDataRecord},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder, ctx := collectionContext(
				t,
				http.MethodPost,
				strings.Repeat(" ", int(test.limit)+1),
			)
			ctx.Request.Header.Set("Content-Type", "application/json")
			ctx.Request.Header.Set("Target-Api-Root", test.target)
			test.invoke(&Server{processor: &collectionContractStub{}}, ctx)
			ctx.Writer.WriteHeaderNow()
			if recorder.Code != http.StatusRequestEntityTooLarge ||
				recorder.Header().Get("Content-Type") != util.ProblemJSONContentType {
				t.Fatalf(
					"status=%d Content-Type=%q body=%s",
					recorder.Code,
					recorder.Header().Get("Content-Type"),
					recorder.Body.String(),
				)
			}
		})
	}
}

func TestMTLFCollectionRoutesForwardPeerProblemDetails(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tests := []struct {
		name   string
		stub   *collectionContractStub
		target string
		body   string
		status int
		cause  string
		invoke func(*Server, *gin.Context)
	}{
		{
			name: "SMF",
			stub: &collectionContractStub{smfErr: &consumer.StandardSmfError{
				StatusCode: http.StatusTooManyRequests,
				ProblemDetails: models.ProblemDetails{
					Status: http.StatusTooManyRequests,
					Cause:  "NF_CONGESTION_RISK",
				},
			}},
			target: "http://smf.example",
			body: `{"nfId":"nwdaf-a","notifId":"corr-a","notifUri":"http://py/callback",` +
				`"eventSubs":[{"event":"UPF_EVENT"}]}`,
			status: http.StatusTooManyRequests,
			cause:  "NF_CONGESTION_RISK",
			invoke: (*Server).CreateSmfEventExposure,
		},
		{
			name: "UDM",
			stub: &collectionContractStub{udmErr: &consumer.StandardUdmError{
				StatusCode: http.StatusNotFound,
				ProblemDetails: models.ProblemDetails{
					Status: http.StatusNotFound,
					Cause:  "DATA_NOT_FOUND",
				},
			}},
			target: "http://udm.example",
			status: http.StatusNotFound,
			cause:  "DATA_NOT_FOUND",
			invoke: (*Server).GetUdmGroupIdentifiers,
		},
		{
			name: "ADRF",
			stub: &collectionContractStub{adrfErr: &consumer.StandardAdrfError{
				StatusCode: http.StatusServiceUnavailable,
				ProblemDetails: models.ProblemDetails{
					Status: http.StatusServiceUnavailable,
					Cause:  "SYSTEM_FAILURE",
				},
			}},
			target: "http://adrf.example",
			body: `{"dataSub":[{"smfDataSub":{}}],` +
				`"dataNotif":{"upfEventNotifs":[{"correlationId":"corr-a"}]}}`,
			status: http.StatusServiceUnavailable,
			cause:  "SYSTEM_FAILURE",
			invoke: (*Server).StoreAdrfDataRecord,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			suffix := ""
			if test.name == "UDM" {
				suffix = "?int-group-id=group-a&ue-id-ind=true"
			}
			recorder, ctx := collectionContext(t, http.MethodPost, test.body, suffix)
			ctx.Request.Header.Set("Content-Type", "application/json")
			ctx.Request.Header.Set("Target-Api-Root", test.target)
			test.invoke(&Server{processor: test.stub}, ctx)
			ctx.Writer.WriteHeaderNow()
			var problem models.ProblemDetails
			if err := json.Unmarshal(recorder.Body.Bytes(), &problem); err != nil {
				t.Fatalf("decode ProblemDetails: %v body=%s", err, recorder.Body.String())
			}
			if recorder.Code != test.status || problem.Cause != test.cause ||
				recorder.Header().Get("Content-Type") != util.ProblemJSONContentType {
				t.Fatalf(
					"status=%d Content-Type=%q body=%s",
					recorder.Code,
					recorder.Header().Get("Content-Type"),
					recorder.Body.String(),
				)
			}
		})
	}
}

func TestMTLFCreateForwardsProvisionalResourceIdentityOnValidationFailure(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder, ctx := collectionContext(t, http.MethodPost, "")
	response := &consumer.StandardSmfResponse{
		StatusCode:          http.StatusCreated,
		Location:            "http://smf.example/nsmf-event-exposure/v1/subscriptions/provisional",
		ContentType:         "application/json",
		Body:                []byte(`{"notifId":"wrong-correlation"}`),
		ProvisionalResource: true,
	}

	(&Server{}).writeSmfEventExposureResponse(
		ctx,
		response,
		errors.New("accepted representation validation failed"),
		false,
	)
	ctx.Writer.WriteHeaderNow()

	if recorder.Code != http.StatusCreated ||
		recorder.Header().Get("Location") != response.Location ||
		recorder.Body.String() != string(response.Body) {
		t.Fatalf(
			"status=%d Location=%q body=%s",
			recorder.Code,
			recorder.Header().Get("Location"),
			recorder.Body.String(),
		)
	}
}

func TestMTLFCreateDoesNotForwardUnownedAcceptedResponse(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder, ctx := collectionContext(t, http.MethodPost, "")
	response := &consumer.StandardSmfResponse{
		StatusCode: http.StatusCreated,
		Location:   "http://smf.example/nsmf-event-exposure/v1/subscriptions/collision",
	}

	(&Server{}).writeSmfEventExposureResponse(
		ctx,
		response,
		errors.New("peer resource route collision"),
		false,
	)
	ctx.Writer.WriteHeaderNow()

	if recorder.Code != http.StatusBadGateway {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestMTLFUdmGroupRejectsRepeatedQueryParameter(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder, ctx := collectionContext(
		t,
		http.MethodGet,
		"",
		"?int-group-id=group-a&int-group-id=group-b&ue-id-ind=true",
	)
	ctx.Request.Header.Set("Target-Api-Root", "http://udm.example")

	(&Server{processor: &collectionContractStub{}}).GetUdmGroupIdentifiers(ctx)
	ctx.Writer.WriteHeaderNow()

	if recorder.Code != http.StatusBadRequest ||
		recorder.Header().Get("Content-Type") != util.ProblemJSONContentType {
		t.Fatalf(
			"status=%d Content-Type=%q body=%s",
			recorder.Code,
			recorder.Header().Get("Content-Type"),
			recorder.Body.String(),
		)
	}
}

func collectionContext(
	t *testing.T,
	method string,
	body string,
	suffix ...string,
) (*httptest.ResponseRecorder, *gin.Context) {
	t.Helper()
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	path := "/"
	if len(suffix) > 0 {
		path += suffix[0]
	}
	ctx.Request = httptest.NewRequestWithContext(
		t.Context(),
		method,
		path,
		strings.NewReader(body),
	)
	return recorder, ctx
}
