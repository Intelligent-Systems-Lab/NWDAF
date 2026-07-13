package processor

import (
	"errors"
	"fmt"

	"github.com/free5gc/nwdaf/internal/anlf/contract"
	"github.com/free5gc/nwdaf/internal/anlf/coordinator"
)

func (p *Processor) HandleRuntimeCompletion(event *contract.RuntimeCompletionEvent) error {
	if p.completionWorkflow == nil {
		return fmt.Errorf("runtime completion workflow is not configured")
	}
	if err := p.completionWorkflow.CompleteSubscriptionRuntime(event); err != nil {
		if errors.Is(err, coordinator.ErrFutureRuntimeRevision) {
			return preserveReportOutcome(ErrFutureRuntimeRevision, err)
		}
		return err
	}
	return nil
}
