package processor

import (
	"context"
	"errors"

	"github.com/free5gc/nwdaf/internal/sbi/consumer"
)

var ErrSmfEventExposureUnavailable = errors.New("SMF Event Exposure proxy is unavailable")

func (p *Processor) CreateSmfEventExposure(
	ctx context.Context,
	targetAPIBaseURI string,
	body []byte,
) (*consumer.StandardSmfResponse, error) {
	if p.smfEventExposure == nil {
		return nil, ErrSmfEventExposureUnavailable
	}
	return p.smfEventExposure.CreateSmfEventExposure(ctx, targetAPIBaseURI, body)
}

func (p *Processor) ReadSmfEventExposure(
	ctx context.Context,
	targetAPIBaseURI string,
	subscriptionID string,
) (*consumer.StandardSmfResponse, error) {
	if p.smfEventExposure == nil {
		return nil, ErrSmfEventExposureUnavailable
	}
	return p.smfEventExposure.ReadSmfEventExposure(ctx, targetAPIBaseURI, subscriptionID)
}

func (p *Processor) ReplaceSmfEventExposure(
	ctx context.Context,
	targetAPIBaseURI string,
	subscriptionID string,
	body []byte,
) (*consumer.StandardSmfResponse, error) {
	if p.smfEventExposure == nil {
		return nil, ErrSmfEventExposureUnavailable
	}
	return p.smfEventExposure.ReplaceSmfEventExposure(ctx, targetAPIBaseURI, subscriptionID, body)
}

func (p *Processor) DeleteSmfEventExposure(
	ctx context.Context,
	targetAPIBaseURI string,
	subscriptionID string,
) (*consumer.StandardSmfResponse, error) {
	if p.smfEventExposure == nil {
		return nil, ErrSmfEventExposureUnavailable
	}
	return p.smfEventExposure.DeleteSmfEventExposure(ctx, targetAPIBaseURI, subscriptionID)
}
