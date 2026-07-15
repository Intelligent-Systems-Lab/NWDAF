package main

import (
	"path/filepath"
	"testing"
)

func TestRunReturnsTerminalStartupError(t *testing.T) {
	missingConfig := filepath.Join(t.TempDir(), "missing.yaml")
	if err := run([]string{"nwdaf", "--config", missingConfig}); err == nil {
		t.Fatal("run() error = nil, want terminal startup error")
	}
	if exitCode := runMain([]string{"nwdaf", "--config", missingConfig}); exitCode != 1 {
		t.Fatalf("runMain() exit code = %d, want 1", exitCode)
	}
}
