package processor

type trainingCompleteWorkflow interface {
	CompleteTrainingTask(taskID, modelURL, status, errMsg string)
}

type Processor struct {
	workflow trainingCompleteWorkflow
}

func NewProcessor(workflow trainingCompleteWorkflow) *Processor {
	return &Processor{workflow: workflow}
}

func (p *Processor) HandleDaisyTrainingComplete(taskID, modelURL, status, errMsg string) {
	if p == nil || p.workflow == nil {
		return
	}
	p.workflow.CompleteTrainingTask(taskID, modelURL, status, errMsg)
}
