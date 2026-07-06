package mtlf

import (
	"context"
	"encoding/json"
	"net/http"
)

// DaisyAPI defines the local Daisy integration seam owned by MTLF.
type DaisyAPI interface {
	TriggerTrainingAsync(ctx context.Context, task map[string]any, callbackURL string, tidOverride string) (string, error)
	UploadData(ctx context.Context, tid string, groupID string, upfEventNotifs []json.RawMessage) error
	HTTPClient() *http.Client
}

const (
	// DaisyPublishTaskPath is the REST API endpoint to trigger FL training on Daisy master.
	DaisyPublishTaskPath = "/publish_task"

	// DaisyUploadDataPath is the REST API endpoint to upload historical training data.
	DaisyUploadDataPath = "/upload_data"

	// DaisyTIDKey is the task ID key in the task payload.
	DaisyTIDKey = "TID"

	// DaisyCallbackURLKey is the key for the callback URL in the task payload.
	DaisyCallbackURLKey = "CALLBACK_URL"
)

// DaisyTrainingCompleteNotification is the callback payload sent by Daisy when async
// training finishes.
type DaisyTrainingCompleteNotification struct {
	TaskID   string `json:"task_id"`
	ModelURL string `json:"model_url,omitempty"`
	Status   string `json:"status"`
	Error    string `json:"error,omitempty"`
}

// DaisyUploadDataRequest is the payload for POST /upload_data.
type DaisyUploadDataRequest struct {
	TID            string            `json:"TID"`
	GroupId        string            `json:"group_id"`
	UpfEventNotifs []json.RawMessage `json:"upfEventNotifs"`
}
