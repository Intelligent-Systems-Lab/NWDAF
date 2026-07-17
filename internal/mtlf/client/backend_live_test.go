package client

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestLiveMtlfBackendReadiness(t *testing.T) {
	endpoint := os.Getenv("MTLF_BACKEND_LIVE_ENDPOINT")
	if endpoint == "" {
		t.Skip("MTLF_BACKEND_LIVE_ENDPOINT is not set")
	}

	backend, err := NewBackendClient(endpoint, 5*time.Second, nil)
	if err != nil {
		t.Fatalf("NewBackendClient() error = %v", err)
	}
	if err = backend.CheckReadiness(context.Background()); err != nil {
		t.Fatalf("CheckReadiness() error = %v", err)
	}
	mode, err := backend.SelectDataSource(
		context.Background(),
		[]DataSource{DataSourceMongoDB},
	)
	if err != nil {
		t.Fatalf("SelectDataSource() error = %v", err)
	}
	if mode != StorageModeMongoDB {
		t.Fatalf("SelectDataSource() mode = %q, want %q", mode, StorageModeMongoDB)
	}
}
