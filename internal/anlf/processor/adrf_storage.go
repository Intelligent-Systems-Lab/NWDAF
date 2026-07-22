package processor

import (
	"context"
	"errors"

	"github.com/free5gc/nwdaf/internal/sbi/consumer"
)

var ErrAdrfStorageUnavailable = errors.New("ADRF storage proxy is unavailable")

func (p *Processor) StoreAdrfDataRecord(
	ctx context.Context,
	body []byte,
) (*consumer.StandardAdrfResponse, error) {
	if p.adrfStorage == nil {
		return nil, ErrAdrfStorageUnavailable
	}
	return p.adrfStorage.StoreAdrfDataRecord(ctx, body)
}
