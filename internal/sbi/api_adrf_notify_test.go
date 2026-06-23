package sbi

import (
	"net/http"
	"testing"

	"go.uber.org/mock/gomock"
)

func TestHandleAdrfRetrievalNotify_InvalidPayload(t *testing.T) {
	server := newHandlerTestServer(nil)
	c, recorder := newJSONRequestContext(http.MethodPost, "/collector/retrieval-notify", []byte(`{"notifCorrId":`))

	server.HandleAdrfRetrievalNotify(c)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
	}

	problem := decodeProblemDetailsResponse(t, recorder)
	if problem.Title != "Malformed request syntax" {
		t.Fatalf("title = %q", problem.Title)
	}
	if problem.Cause != "" {
		t.Fatalf("cause = %q, want empty", problem.Cause)
	}
}

func TestHandleAdrfRetrievalNotify_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockProcessor := NewMockprocessorAPI(ctrl)
	mockProcessor.EXPECT().
		HandleAdrfRetrievalNotify("corr-123", []string{"fetch-1", "fetch-2"}, true)

	server := newHandlerTestServer(mockProcessor)
	notifyBody := `{
		"notifCorrId":"corr-123",
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
