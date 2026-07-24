package processor

import (
	"context"
	"errors"

	"github.com/free5gc/nwdaf/internal/sbi/consumer"
)

var ErrNFDiscoveryUnavailable = errors.New("NF discovery proxy is unavailable")

func (p *Processor) HandleNFDiscovery(
	ctx context.Context,
	query consumer.NFDiscoveryQuery,
) (*consumer.NFDiscoveryResult, error) {
	if p.nfDiscovery == nil {
		return nil, ErrNFDiscoveryUnavailable
	}
	return p.nfDiscovery.DiscoverNFInstances(ctx, query)
}
