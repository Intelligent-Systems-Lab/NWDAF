package sbi

import (
	"net/http"
	"testing"
	"time"

	"go.uber.org/mock/gomock"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/openapi/models"
)

func TestHandleMlModelProvisionNotify_InvalidJSON(t *testing.T) {
	server := newHandlerTestServer(nil)
	c, recorder := newJSONRequestContext(http.MethodPost, "/mlmodel-notify", []byte(`{"subscriptionId":`))

	server.HandleMlModelProvisionNotify(c)

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

func TestHandleMlModelProvisionNotify_EmptyNotificationList(t *testing.T) {
	server := newHandlerTestServer(nil)
	c, recorder := newJSONRequestContext(http.MethodPost, "/mlmodel-notify", []byte(`[]`))

	server.HandleMlModelProvisionNotify(c)
	c.Writer.WriteHeaderNow()

	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNoContent)
	}
}

func TestHandleMlModelProvisionNotify_InitializesModel(t *testing.T) {
	nwdaf_context.Init()
	ctx := nwdaf_context.GetSelf()
	ctx.SetMlModelInfo("sub-123", nwdaf_context.NewMlModelInfo(models.NwdafEvent_UE_COMMUNICATION, "mtlf"))

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	done := make(chan struct{})
	mockProcessor := NewMockprocessorAPI(ctrl)
	mockProcessor.EXPECT().
		InitializeMlModel("sub-123", gomock.Any(), "http://example.com/model.onnx").
		DoAndReturn(func(string, *nwdaf_context.MlModelInfo, string) {
			close(done)
		})

	server := newHandlerTestServer(mockProcessor)
	notificationBody := `[
		{
			"subscriptionId":"mtlf-sub-1",
			"eventNotifs":[
				{
					"event":"UE_COMMUNICATION",
					"notifCorreId":"sub-123",
					"mLFileAddr":{"mLModelUrl":"http://example.com/model.onnx"}
				}
			]
		}
	]`
	c, recorder := newJSONRequestContext(
		http.MethodPost,
		"/mlmodel-notify",
		[]byte(notificationBody),
	)

	server.HandleMlModelProvisionNotify(c)
	c.Writer.WriteHeaderNow()

	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNoContent)
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("expected InitializeMlModel to be invoked")
	}
}
