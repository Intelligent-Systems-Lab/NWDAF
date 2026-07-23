package backend

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/free5gc/openapi/models"
)

func TestStandardProblemDetailsUsesActualHTTPStatus(t *testing.T) {
	t.Parallel()

	backendError := &StandardError{
		StatusCode: http.StatusServiceUnavailable,
		ProblemDetails: models.ProblemDetails{
			Status: http.StatusBadRequest,
			Cause:  "SYSTEM_FAILURE",
		},
	}
	problem := backendError.StandardProblemDetails()
	if problem.Status != http.StatusServiceUnavailable {
		t.Fatalf("ProblemDetails status = %d, want %d", problem.Status, http.StatusServiceUnavailable)
	}
}

func TestExecuteStandardRequestOnlyAcceptsDeclaredRedirects(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Location", "http://peer.example/resource")
		response.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer server.Close()

	_, err := ExecuteStandardRequest(
		context.Background(),
		server.Client(),
		time.Second,
		http.MethodPost,
		server.URL,
		[]byte(`{}`),
		"create resource",
		StandardOperationContract{
			SuccessValidators: map[int]func([]byte) error{http.StatusCreated: nil},
		},
	)
	var contractError *ContractError
	if !errors.As(err, &contractError) {
		t.Fatalf("create redirect error = %T %v, want ContractError", err, err)
	}

	_, err = ExecuteStandardRequest(
		context.Background(),
		server.Client(),
		time.Second,
		http.MethodDelete,
		server.URL,
		nil,
		"delete resource",
		StandardOperationContract{
			SuccessValidators: map[int]func([]byte) error{
				http.StatusNoContent: nil,
			},
		},
	)
	if !errors.As(err, &contractError) {
		t.Fatalf("private backend redirect error = %T %v, want ContractError", err, err)
	}
}

func TestExecuteStandardRequestRejectsUndeclaredProblemStatus(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "application/problem+json")
		response.WriteHeader(http.StatusTeapot)
		if _, err := response.Write([]byte(`{"status":418,"cause":"UNDECLARED"}`)); err != nil {
			t.Errorf("write response: %v", err)
		}
	}))
	defer server.Close()

	_, err := ExecuteStandardRequest(
		context.Background(),
		server.Client(),
		time.Second,
		http.MethodPost,
		server.URL,
		[]byte(`{}`),
		"create resource",
		StandardOperationContract{
			SuccessValidators: map[int]func([]byte) error{http.StatusCreated: nil},
			ErrorStatuses:     ErrorStatuses(http.StatusBadRequest),
		},
	)
	var contractError *ContractError
	if !errors.As(err, &contractError) {
		t.Fatalf("undeclared status error = %T %v, want ContractError", err, err)
	}
}

func TestExecuteStandardRequestFollowsGoOwnedStandardRedirect(t *testing.T) {
	t.Parallel()

	final := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.WriteHeader(http.StatusNoContent)
	}))
	defer final.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Location", final.URL)
		response.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()

	response, err := ExecuteStandardRequest(
		context.Background(),
		redirect.Client(),
		time.Second,
		http.MethodPost,
		redirect.URL,
		[]byte(`{}`),
		"deliver standard callback",
		StandardOperationContract{
			SuccessValidators: map[int]func([]byte) error{http.StatusNoContent: nil},
			FollowRedirects:   true,
		},
	)
	if err != nil || response == nil || response.StatusCode != http.StatusNoContent {
		t.Fatalf("followed response = %+v, error = %v", response, err)
	}
}
