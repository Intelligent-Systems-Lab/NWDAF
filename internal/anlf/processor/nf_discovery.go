package processor

import (
	"context"
	"errors"

	"github.com/free5gc/openapi/models"
)

var ErrNFDiscoveryUnavailable = errors.New("NF discovery proxy is unavailable")

func (p *Processor) HandleSmfNFDiscovery(ctx context.Context) (*models.SearchResult, error) {
	if p.nfDiscovery == nil {
		return nil, ErrNFDiscoveryUnavailable
	}
	return p.nfDiscovery.DiscoverSmfProfiles(ctx)
}
