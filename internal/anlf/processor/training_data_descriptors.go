package processor

import (
	"context"
	"errors"
)

var ErrTrainingDataDescriptorUnavailable = errors.New("training-data descriptor relay is unavailable")

func (p *Processor) PutTrainingDataDescriptor(
	ctx context.Context,
	descriptorID string,
	body []byte,
) error {
	if p == nil || p.trainingDataRelay == nil || p.mtlfAvailability == nil {
		return ErrTrainingDataDescriptorUnavailable
	}
	lease, admitted := p.mtlfAvailability.Acquire()
	if !admitted {
		return ErrTrainingDataDescriptorUnavailable
	}
	if lease != nil {
		defer lease.Release()
	}
	_, err := p.trainingDataRelay.PutTrainingDataDescriptor(ctx, descriptorID, body)
	return err
}

func (p *Processor) DeleteTrainingDataDescriptor(
	ctx context.Context,
	descriptorID string,
) error {
	if p == nil || p.trainingDataRelay == nil || p.mtlfAvailability == nil {
		return ErrTrainingDataDescriptorUnavailable
	}
	lease, admitted := p.mtlfAvailability.Acquire()
	if !admitted {
		return ErrTrainingDataDescriptorUnavailable
	}
	if lease != nil {
		defer lease.Release()
	}
	_, err := p.trainingDataRelay.DeleteTrainingDataDescriptor(ctx, descriptorID)
	return err
}
