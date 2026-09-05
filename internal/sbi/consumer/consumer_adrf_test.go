package consumer

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAdrfClientForTargetReusesNormalizedTarget(t *testing.T) {
	consumer := newConsumerWithServices(nil, nil)

	first := consumer.adrfClientForTarget(" http://adrf.example/ ")
	second := consumer.adrfClientForTarget("http://adrf.example")
	other := consumer.adrfClientForTarget("http://other-adrf.example")

	if first != second {
		t.Fatal("equivalent ADRF targets did not reuse one transport client")
	}
	if first == other {
		t.Fatal("different ADRF targets reused the same transport client")
	}
}

func TestConsumerDelegatesAdrfMLModelRecordMutations(t *testing.T) {
	body := []byte(`{
		"nfInstanceId":"11111111-1111-4111-8111-111111111111",
		"mlModelInfo":[{
			"modelUniqueId":42,
			"mlFileAddr":{"mLModelUrl":"http://root.example/models/round-a"},
			"mlStorageSize":128
		}]
	}`)
	requestNumber := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requestNumber++
		if request.URL.Path != AdrfMLModelStoreRecordsPath+"/round-record-a" {
			t.Errorf("path = %q", request.URL.Path)
		}
		switch requestNumber {
		case 1:
			if request.Method != http.MethodPut {
				t.Errorf("method = %q, want PUT", request.Method)
			}
			gotBody, err := io.ReadAll(request.Body)
			if err != nil || !bytes.Equal(gotBody, body) {
				t.Errorf("body=%q error=%v", string(gotBody), err)
			}
			writer.WriteHeader(http.StatusNoContent)
		case 2:
			if request.Method != http.MethodDelete {
				t.Errorf("method = %q, want DELETE", request.Method)
			}
			writer.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected request %d", requestNumber)
		}
	}))
	t.Cleanup(server.Close)

	consumer := newConsumerWithServices(nil, nil)
	response, err := consumer.UpdateAdrfMLModelRecord(
		context.Background(),
		server.URL,
		"round-record-a",
		body,
	)
	if err != nil || response == nil || response.StatusCode != http.StatusNoContent {
		t.Fatalf("update response=%+v error=%v", response, err)
	}
	response, err = consumer.DeleteAdrfMLModelRecord(
		context.Background(),
		server.URL,
		"round-record-a",
	)
	if err != nil || response == nil || response.StatusCode != http.StatusNoContent {
		t.Fatalf("delete response=%+v error=%v", response, err)
	}
	if requestNumber != 2 {
		t.Fatalf("request count = %d, want 2", requestNumber)
	}
}

func TestConsumerRejectsMissingAdrfMLModelMutationTarget(t *testing.T) {
	consumer := newConsumerWithServices(nil, nil)
	if response, err := consumer.UpdateAdrfMLModelRecord(
		context.Background(),
		"",
		"round-record-a",
		[]byte(`{}`),
	); err == nil || response != nil {
		t.Fatalf("update response=%+v error=%v", response, err)
	}
	if response, err := consumer.DeleteAdrfMLModelRecord(
		context.Background(),
		"",
		"round-record-a",
	); err == nil || response != nil {
		t.Fatalf("delete response=%+v error=%v", response, err)
	}
}
