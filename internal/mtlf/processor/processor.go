package processor

import "github.com/free5gc/nwdaf/internal/mtlf"

type trainingCompleteWorkflow interface {
	TakeTrainingCompletion(taskID string) (mtlf.TrainingCompletion, bool)
	HandleFailedTrainingCompletion(taskID string, completion mtlf.TrainingCompletion, errMsg string)
	HandleSuccessfulTrainingCompletion(taskID string, completion mtlf.TrainingCompletion, modelURL string)
}

type Processor struct {
	workflow trainingCompleteWorkflow
}

func NewProcessor(workflow trainingCompleteWorkflow) *Processor {
	return &Processor{workflow: workflow}
}
