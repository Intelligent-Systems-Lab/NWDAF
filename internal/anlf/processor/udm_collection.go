package processor

import (
	"context"
	"errors"

	"github.com/free5gc/nwdaf/internal/sbi/consumer"
	"github.com/free5gc/openapi/models"
)

var ErrUdmCollectionUnavailable = errors.New("UDM collection proxy is unavailable")

func (p *Processor) GetUdmGroupIdentifiers(
	ctx context.Context,
	targetAPIBaseURI string,
	intGroupID string,
	ueIDInd bool,
) (*consumer.StandardUdmResponse, error) {
	if p.udmCollection == nil {
		return nil, ErrUdmCollectionUnavailable
	}
	return p.udmCollection.GetUdmGroupIdentifiers(ctx, targetAPIBaseURI, intGroupID, ueIDInd)
}

func (p *Processor) GetUdmSmfRegistration(
	ctx context.Context,
	targetAPIBaseURI string,
	ueID string,
	singleNssai *models.Snssai,
	dnn string,
) (*consumer.StandardUdmResponse, error) {
	if p.udmCollection == nil {
		return nil, ErrUdmCollectionUnavailable
	}
	return p.udmCollection.GetUdmSmfRegistration(ctx, targetAPIBaseURI, ueID, singleNssai, dnn)
}
