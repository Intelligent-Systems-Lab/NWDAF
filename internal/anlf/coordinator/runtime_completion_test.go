package coordinator

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/free5gc/nwdaf/internal/anlf/contract"
	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/pkg/factory"
)

func completionEvent(subscriptionID string, revision int64) *contract.RuntimeCompletionEvent {
	return &contract.RuntimeCompletionEvent{
		CompletionID:       "completion-1",
		SubscriptionID:     subscriptionID,
		RuntimeRevision:    revision,
		Reason:             contract.RuntimeCompletionMaxReportsReached,
		CompletedAt:        time.Date(2026, 7, 14, 0, 0, 0, 0, time.UTC),
		LastReportSequence: 1,
	}
}

func TestCompleteSubscriptionRuntimeRevisionSemantics(t *testing.T) {
	tests := []struct {
		name       string
		current    int64
		completion int64
		active     bool
		wantErr    error
		wantActive bool
	}{
		{name: "current", current: 2, completion: 2, active: true, wantActive: false},
		{name: "duplicate", current: 2, completion: 2, active: false, wantActive: false},
		{name: "stale", current: 3, completion: 2, active: true, wantActive: true},
		{name: "future", current: 2, completion: 3, active: true, wantErr: ErrFutureRuntimeRevision, wantActive: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			nwdaf_context.Init()
			subscription := &nwdaf_context.Subscription{
				ID:              "sub-1",
				RuntimeRevision: test.current,
				IsActive:        test.active,
			}
			nwdaf_context.GetSelf().AddSubscription(subscription)
			service := newTestCoordinator(testNwdafApp{ctx: context.Background()}, nil)

			err := service.CompleteSubscriptionRuntime(completionEvent("sub-1", test.completion))
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("error = %v, want %v", err, test.wantErr)
			}
			_, _, _, active := subscription.RuntimeSnapshot()
			if active != test.wantActive {
				t.Fatalf("active = %v, want %v", active, test.wantActive)
			}
		})
	}
}

func TestCompleteSubscriptionRuntimeConsumesMissingSubscription(t *testing.T) {
	nwdaf_context.Init()
	service := newTestCoordinator(testNwdafApp{ctx: context.Background()}, nil)

	if err := service.CompleteSubscriptionRuntime(completionEvent("missing", 1)); err != nil {
		t.Fatalf("missing subscription completion error = %v", err)
	}
}

func TestOldCompletionConcurrentWithContextReplacementKeepsNewRuntimeActive(t *testing.T) {
	for range 100 {
		nwdaf_context.Init()
		oldSubscription := &nwdaf_context.Subscription{
			ID:              "sub-1",
			RuntimeRevision: 1,
			IsActive:        true,
		}
		nwdaf_context.GetSelf().AddSubscription(oldSubscription)
		service := newTestCoordinator(testNwdafApp{ctx: context.Background()}, nil)
		newSubscription := &nwdaf_context.Subscription{
			ID:              "sub-1",
			RuntimeRevision: 2,
			IsActive:        true,
		}
		start := make(chan struct{})
		done := make(chan error, 2)

		go func() {
			<-start
			done <- service.CompleteSubscriptionRuntime(completionEvent("sub-1", 1))
		}()
		go func() {
			<-start
			nwdaf_context.GetSelf().UpdateSubscription(newSubscription)
			done <- nil
		}()
		close(start)
		for range 2 {
			if err := <-done; err != nil {
				t.Fatalf("runtime completion error = %v", err)
			}
		}

		current := nwdaf_context.GetSelf().GetSubscription("sub-1")
		revision, _, _, active := current.RuntimeSnapshot()
		if revision != 2 || !active {
			t.Fatalf("new runtime state = revision %d active %v", revision, active)
		}
	}
}

func TestBuildRuntimeCompletionCallbackURIUsesAnlfServerConfig(t *testing.T) {
	service := newTestCoordinator(testNwdafApp{ctx: context.Background(), cfg: &factory.Config{
		Configuration: &factory.Configuration{Anlf: &factory.AnlfConfig{
			Server: &factory.AuxiliaryServerConfig{
				BindingIPv4:  "127.0.0.1",
				RegisterIPv4: "10.0.0.8",
				Port:         9010,
			},
		}},
	}}, nil)

	want := "http://10.0.0.8:9010/subscriptions/sub-1/runtime-completions"
	if got := service.BuildRuntimeCompletionCallbackURI("sub-1"); got != want {
		t.Fatalf("BuildRuntimeCompletionCallbackURI() = %q", got)
	}
}
