package processor

import (
	"fmt"

	"github.com/free5gc/nwdaf/internal/anlf/contract"
)

func (p *Processor) HandleModelAccuracyReport(report *contract.ModelAccuracyReport) error {
	if p.accuracyWorkflow == nil {
		return fmt.Errorf("MTLF accuracy workflow not initialized")
	}
	return p.accuracyWorkflow.HandleModelAccuracyReport(report)
}
