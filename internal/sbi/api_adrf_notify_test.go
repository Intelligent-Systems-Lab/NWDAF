package sbi

import (
	"context"
	"net/http"
	"testing"

	"go.uber.org/mock/gomock"

	"github.com/free5gc/nwdaf/internal/backend"
)

func TestHandleAdrfRetrievalNotify_InvalidPayload(t *testing.T) {
	server := newHandlerTestServer(t, nil)
	c, recorder := newJSONRequestContext(http.MethodPost, "/collector/retrieval-notify", []byte(`{"notifCorrId":`))

	server.HandleAdrfRetrievalNotify(c)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
	}

	problem := decodeProblemDetailsResponse(t, recorder)
	if problem.Title != http.StatusText(http.StatusBadRequest) {
		t.Fatalf("title = %q", problem.Title)
	}
	if problem.Cause != "INVALID_MSG_FORMAT" {
		t.Fatalf("cause = %q", problem.Cause)
	}
}

func TestHandleAdrfRetrievalNotify_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockProcessor := NewMockprocessorAPI(ctrl)
	mockProcessor.EXPECT().
		HandleAdrfRetrievalNotify(gomock.AssignableToTypeOf(context.Background()), gomock.Any()).
		Return(&backend.StandardResponse{StatusCode: http.StatusNoContent}, nil)

	server := newHandlerTestServer(t, mockProcessor)
	notifyBody := `{
		"notifCorrId":"corr-123",
		"timeStamp":"2026-07-24T00:00:00Z",
		"terminationReq":true,
		"fetchInstruct":{
			"fetchUri":"http://adrf.example/fetch",
			"fetchCorrIds":["fetch-1","fetch-2"]
		}
	}`
	c, recorder := newJSONRequestContext(
		http.MethodPost,
		"/collector/retrieval-notify",
		[]byte(notifyBody),
	)

	server.HandleAdrfRetrievalNotify(c)
	c.Writer.WriteHeaderNow()

	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNoContent)
	}
}

func TestHandleAdrfRetrievalNotify_RejectsJSONPrefixMediaType(t *testing.T) {
	server := newHandlerTestServer(t, nil)
	c, recorder := newJSONRequestContext(
		http.MethodPost,
		"/collector/retrieval-notify",
		[]byte(`{"notifCorrId":"corr","timeStamp":"2026-07-24T00:00:00Z"}`),
	)
	c.Request.Header.Set("Content-Type", "application/jsonx")

	server.HandleAdrfRetrievalNotify(c)

	if recorder.Code != http.StatusUnsupportedMediaType {
		t.Fatalf(
			"status = %d, want %d",
			recorder.Code,
			http.StatusUnsupportedMediaType,
		)
	}
}
