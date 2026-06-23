package sbi

import (
	"errors"
	"net/http"
	"testing"

	"go.uber.org/mock/gomock"

	"github.com/free5gc/nwdaf/internal/sbi/processor"
	"github.com/free5gc/openapi/models"
)

func TestHandleCollectorNotify_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockProcessor := NewMockprocessorAPI(ctrl)
	mockProcessor.EXPECT().
		HandleSmfNotification(gomock.AssignableToTypeOf(&models.NsmfEventExposureNotification{})).
		Return(nil)

	server := newHandlerTestServer(mockProcessor)
	c, recorder := newJSONRequestContext(
		http.MethodPost,
		"/collector/notify",
		[]byte(`{"notifId":"corr-123","eventNotifs":[]}`),
	)

	server.HandleCollectorNotify(c)
	c.Writer.WriteHeaderNow()

	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNoContent)
	}
}

func TestHandleCollectorNotify_InvalidJSON(t *testing.T) {
	server := newHandlerTestServer(nil)
	c, recorder := newJSONRequestContext(http.MethodPost, "/collector/notify", []byte(`{"notifId":`))

	server.HandleCollectorNotify(c)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
	}

	problem := decodeProblemDetailsResponse(t, recorder)
	if problem.Title != malformedRequestSyntaxTitle {
		t.Fatalf("title = %q", problem.Title)
	}
	if problem.Cause != "" {
		t.Fatalf("cause = %q, want empty", problem.Cause)
	}
}

func TestHandleUpfNotify_ProcessorFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockProcessor := NewMockprocessorAPI(ctrl)
	mockProcessor.EXPECT().
		HandleUpfNotification(gomock.AssignableToTypeOf(&processor.UpfNotificationData{})).
		Return(errors.New("processor failed"))

	server := newHandlerTestServer(mockProcessor)
	upfNotifyBody := `{
		"correlationId":"corr-123",
		"notificationItems":[
			{
				"eventType":"USER_DATA_USAGE_MEASURES",
				"ueIpv4Addr":"10.0.0.1",
				"timeStamp":"2026-06-23T00:00:00Z",
				"startTime":"2026-06-23T00:00:00Z"
			}
		]
	}`
	c, recorder := newJSONRequestContext(
		http.MethodPost,
		"/collector/upf-notify",
		[]byte(upfNotifyBody),
	)

	server.HandleUpfNotify(c)

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusInternalServerError)
	}

	problem := decodeProblemDetailsResponse(t, recorder)
	if problem.Title != "System failure" {
		t.Fatalf("title = %q", problem.Title)
	}
	if problem.Cause != "SYSTEM_FAILURE" {
		t.Fatalf("cause = %q, want %q", problem.Cause, "SYSTEM_FAILURE")
	}
}
