package consumer

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestUdmStandardProxyPreservesStandardPathsAndErrors(t *testing.T) {
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.RequestURI())
		switch r.URL.Path {
		case "/nudm-sdm/v2/group-data/group-identifiers":
			w.Header().Set("Content-Type", "application/json")
			if _, err := w.Write([]byte(
				`{"intGroupId":"00000001-466-92-01",` +
					`"ueIdList":[{"supi":"imsi-466920000000001"}]}`,
			)); err != nil {
				t.Errorf("write group response: %v", err)
			}
		case "/nudm-uecm/v1/imsi-466920000000001/registrations/smf-registrations":
			w.Header().Set("Content-Type", "application/problem+json")
			w.WriteHeader(http.StatusNotFound)
			if _, err := w.Write([]byte(`{"status":404,"cause":"DATA_NOT_FOUND"}`)); err != nil {
				t.Errorf("write registration response: %v", err)
			}
		default:
			t.Fatalf("unexpected path %s", r.URL.RequestURI())
		}
	}))
	defer server.Close()

	consumer, err := NewConsumer(newTestConsumerApp(nil))
	if err != nil {
		t.Fatalf("NewConsumer() error = %v", err)
	}
	consumer.mlModelPeerHTTPClient = server.Client()
	group, err := consumer.GetUdmGroupIdentifiers(t.Context(), server.URL, "00000001-466-92-01", true)
	if err != nil || group.StatusCode != http.StatusOK {
		t.Fatalf("group response=%+v err=%v", group, err)
	}
	_, err = consumer.GetUdmSmfRegistration(t.Context(), server.URL, "imsi-466920000000001", nil, "")
	standardError, ok := err.(*StandardUdmError)
	if !ok || standardError.ProblemDetails.Cause != "DATA_NOT_FOUND" {
		t.Fatalf("registration error=%T %+v", err, err)
	}
	wantGroupPath := "/nudm-sdm/v2/group-data/group-identifiers?" +
		"int-group-id=00000001-466-92-01&ue-id-ind=true"
	if len(paths) != 2 || paths[0] != wantGroupPath {
		t.Fatalf("paths=%v", paths)
	}
}
