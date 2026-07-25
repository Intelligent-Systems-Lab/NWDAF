package anlf

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	anlfprocessor "github.com/free5gc/nwdaf/internal/anlf/processor"
	"github.com/free5gc/nwdaf/internal/backend"
)

type associationProcessorStub struct {
	err    error
	update backend.SmfResourceAssociationUpdate
}

func (s *associationProcessorStub) ReplaceSmfResourceAssociations(
	update backend.SmfResourceAssociationUpdate,
) error {
	s.update = update
	return s.err
}

func TestReplaceSmfResourceAssociationsStatusAndValidation(t *testing.T) {
	gin.SetMode(gin.TestMode)
	body := `{"processInstanceId":"fa4f6f99-436a-4381-9f20-dfceaa587480",` +
		`"smfResources":[{"targetApiRoot":"http://smf.example/",` +
		`"peerSubscriptionId":"peer-a","nwdafSubscriptionIds":` +
		`["72a84547-dde2-4cb8-bc75-d6c2de2eb4c9"]}]}`
	tests := []struct {
		name       string
		err        error
		body       string
		wantStatus int
	}{
		{name: "accepted", body: body, wantStatus: http.StatusNoContent},
		{name: "stale process", body: body, err: anlfprocessor.ErrStaleBackendProcess, wantStatus: http.StatusConflict},
		{name: "unknown peer", body: body, err: anlfprocessor.ErrUnknownSmfResource, wantStatus: http.StatusConflict},
		{
			name: "backend syncing", body: body, err: anlfprocessor.ErrBackendUnavailable,
			wantStatus: http.StatusServiceUnavailable,
		},
		{name: "malformed UUID", body: `{"processInstanceId":"bad","smfResources":[]}`, wantStatus: http.StatusBadRequest},
		{
			name:       "missing resources",
			body:       `{"processInstanceId":"fa4f6f99-436a-4381-9f20-dfceaa587480"}`,
			wantStatus: http.StatusBadRequest,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stub := &associationProcessorStub{err: test.err}
			server := &Server{processor: stub}
			recorder := httptest.NewRecorder()
			context, _ := gin.CreateTestContext(recorder)
			context.Request = httptest.NewRequest(http.MethodPut, "/", strings.NewReader(test.body))

			server.ReplaceSmfResourceAssociations(context)
			context.Writer.WriteHeaderNow()

			if recorder.Code != test.wantStatus {
				t.Fatalf(
					"status = %d, want %d, body=%s",
					recorder.Code,
					test.wantStatus,
					recorder.Body.String(),
				)
			}
			if test.wantStatus == http.StatusNoContent {
				if stub.update.SmfResources[0].TargetAPIBaseURI != "http://smf.example" {
					t.Fatalf("normalized target = %q", stub.update.SmfResources[0].TargetAPIBaseURI)
				}
			} else if test.err != nil && !errors.Is(stub.err, test.err) {
				t.Fatalf("stub error = %v", stub.err)
			}
		})
	}
}
