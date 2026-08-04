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
	err              error
	update           backend.SmfResourceAssociationUpdate
	descriptorUpdate backend.TrainingDataDescriptorUpdate
}

func (s *associationProcessorStub) ReplaceTrainingDataDescriptors(
	update backend.TrainingDataDescriptorUpdate,
) error {
	s.descriptorUpdate = update
	return s.err
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
			context.Request = httptest.NewRequestWithContext(
				t.Context(),
				http.MethodPut,
				"/",
				strings.NewReader(test.body),
			)

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

func TestReplaceTrainingDataDescriptorsUsesIndependentTypedSnapshot(t *testing.T) {
	gin.SetMode(gin.TestMode)
	body := `{
  "processInstanceId":"fa4f6f99-436a-4381-9f20-dfceaa587480",
  "trainingDataDescriptors":[{
    "correlationId":"22222222-2222-4222-8222-222222222222",
    "state":"RETAINED",
    "storedDataSpec":{
      "dataSpec":{"smfDataSub":{"supi":"imsi-466920000000001","notifId":"corr-a",
        "notifUri":"http://anlf.example/callback","eventSubs":[{"event":"UPF_EVENT"}]}},
      "timePeriod":{"startTime":"2026-08-04T09:00:00Z","stopTime":"2026-08-04T09:30:00Z"}
    },
    "mlEventSubscription":{"mLEvent":"UE_COMMUNICATION","tgtUe":{"intGroupIds":["00000001-466-92-01"]}},
    "sourceNfInstanceId":"11111111-1111-4111-8111-111111111111",
    "adrfInstanceId":"33333333-3333-4333-8333-333333333333",
    "retainUntil":"2099-08-04T10:30:00Z"
  }]
}`
	stub := &associationProcessorStub{}
	server := &Server{processor: stub}
	recorder := httptest.NewRecorder()
	context, _ := gin.CreateTestContext(recorder)
	context.Request = httptest.NewRequestWithContext(
		t.Context(), http.MethodPut, "/", strings.NewReader(body),
	)

	server.ReplaceTrainingDataDescriptors(context)
	context.Writer.WriteHeaderNow()

	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status = %d, body=%s", recorder.Code, recorder.Body.String())
	}
	if len(stub.descriptorUpdate.TrainingDataDescriptors) != 1 ||
		stub.descriptorUpdate.TrainingDataDescriptors[0].State != "RETAINED" {
		t.Fatalf("descriptor update = %+v", stub.descriptorUpdate)
	}
}
