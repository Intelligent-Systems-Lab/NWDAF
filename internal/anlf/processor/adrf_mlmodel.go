package processor

import (
	"context"

	"github.com/free5gc/nwdaf/internal/sbi/consumer"
)

func (p *Processor) RetrieveAdrfMLModelRecord(
	ctx context.Context,
	targetAPIBaseURI string,
	storeTransID string,
	modelUniqueIDs []int64,
) (*consumer.StandardAdrfResponse, error) {
	if p.adrfStorage == nil {
		return nil, ErrAdrfStorageUnavailable
	}
	return p.adrfStorage.RetrieveAdrfMLModelRecord(
		ctx,
		targetAPIBaseURI,
		storeTransID,
		modelUniqueIDs,
	)
}
