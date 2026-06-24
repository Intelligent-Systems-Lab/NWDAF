package sbi

import (
	"net/http"
	"testing"

	"github.com/gin-gonic/gin"
	"go.uber.org/mock/gomock"

	"github.com/free5gc/nwdaf/internal/util"
	"github.com/free5gc/openapi/models"
)

func TestHandleCreateSubscription_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockProcessor := NewMockprocessorAPI(ctrl)
	mockProcessor.EXPECT().
		HandleCreateSubscription(gomock.AssignableToTypeOf(&models.NnwdafEventsSubscription{})).
		Return(&models.NnwdafEventsSubscription{}, "sub-123", nil)

	server := newHandlerTestServer(t, mockProcessor)
	createBody := `{
		"notificationURI":"http://consumer.example/callback",
		"eventSubscriptions":[
			{"event":"UE_COMMUNICATION","tgtUe":{"supis":["imsi-001"]}}
		]
	}`
	c, recorder := newJSONRequestContext(
		http.MethodPost,
		"/nnwdaf-eventssubscription/v1/subscriptions",
		[]byte(createBody),
	)

	server.HandleCreateSubscription(c)

	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusCreated)
	}
	const expectedLocation = "http://127.0.0.1:8080/nnwdaf-eventssubscription/v1/subscriptions/sub-123"
	if location := recorder.Header().Get("Location"); location != expectedLocation {
		t.Fatalf("Location = %q", location)
	}
}

func TestHandleCreateSubscription_InvalidJSON(t *testing.T) {
	server := newHandlerTestServer(t, nil)
	c, recorder := newJSONRequestContext(
		http.MethodPost,
		"/nnwdaf-eventssubscription/v1/subscriptions",
		[]byte(`{"notificationURI":`),
	)

	server.HandleCreateSubscription(c)

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

func TestHandleUpdateSubscription_ProcessorFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockProcessor := NewMockprocessorAPI(ctrl)
	mockProcessor.EXPECT().
		HandleUpdateSubscription("sub-123", gomock.AssignableToTypeOf(&models.NnwdafEventsSubscription{})).
		Return(nil, &models.ProblemDetails{
			Status: http.StatusNotFound,
			Cause:  "SUBSCRIPTION_NOT_FOUND",
		})

	server := newHandlerTestServer(t, mockProcessor)
	updateBody := `{
		"notificationURI":"http://consumer.example/callback",
		"eventSubscriptions":[
			{"event":"UE_COMMUNICATION","tgtUe":{"supis":["imsi-001"]}}
		]
	}`
	c, recorder := newJSONRequestContext(
		http.MethodPut,
		"/nnwdaf-eventssubscription/v1/subscriptions/sub-123",
		[]byte(updateBody),
	)
	c.Params = []gin.Param{{Key: "subscriptionId", Value: "sub-123"}}

	server.HandleUpdateSubscription(c)

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNotFound)
	}
	if contentType := recorder.Header().Get("Content-Type"); contentType != util.ProblemJSONContentType {
		t.Fatalf("Content-Type = %q, want %q", contentType, util.ProblemJSONContentType)
	}
}

func TestHandleDeleteSubscription_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockProcessor := NewMockprocessorAPI(ctrl)
	mockProcessor.EXPECT().HandleDeleteSubscription("sub-123").Return(nil)

	server := newHandlerTestServer(t, mockProcessor)
	c, recorder := newJSONRequestContext(http.MethodDelete, "/nnwdaf-eventssubscription/v1/subscriptions/sub-123", nil)
	c.Params = []gin.Param{{Key: "subscriptionId", Value: "sub-123"}}

	server.HandleDeleteSubscription(c)
	c.Writer.WriteHeaderNow()

	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNoContent)
	}
}
