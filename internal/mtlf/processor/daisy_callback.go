package processor

func (p *Processor) HandleDaisyTrainingComplete(taskID, modelURL, status, errMsg string) {
	if p == nil || p.workflow == nil {
		return
	}

	completion, ok := p.workflow.TakeTrainingCompletion(taskID)
	if !ok {
		return
	}

	if status != "success" {
		p.workflow.HandleFailedTrainingCompletion(taskID, completion, errMsg)
		return
	}

	p.workflow.HandleSuccessfulTrainingCompletion(taskID, completion, modelURL)
}
