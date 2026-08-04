package processor

import (
	"encoding/json"
	"errors"

	"github.com/free5gc/nwdaf/internal/backend"
	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
)

var (
	ErrBackendUnavailable  = errors.New("AnLF backend is unavailable")
	ErrStaleBackendProcess = errors.New("AnLF backend process instance is stale")
	ErrUnknownSmfResource  = errors.New("SMF resource association references an unknown peer resource")
)

type availabilitySnapshot interface {
	Snapshot() backend.Snapshot
}

type syncRefresher interface {
	Refresh()
}

func (p *Processor) SetSmfAssociationRepository(
	nwdafContext *nwdaf_context.NWDAFContext,
	availability availabilitySnapshot,
	mtlfSyncRefresher syncRefresher,
) {
	p.nwdafContext = nwdafContext
	p.availability = availability
	p.mtlfSyncRefresher = mtlfSyncRefresher
}

func (p *Processor) ReplaceSmfResourceAssociations(
	update backend.SmfResourceAssociationUpdate,
) error {
	if p == nil || p.nwdafContext == nil || p.availability == nil {
		return ErrBackendUnavailable
	}
	snapshot := p.availability.Snapshot()
	if snapshot.State != backend.StateUsable {
		return ErrBackendUnavailable
	}
	if update.ProcessInstanceID == "" || update.ProcessInstanceID != snapshot.ProcessInstanceID {
		return ErrStaleBackendProcess
	}
	associations := make([]nwdaf_context.SmfPeerResourceAssociation, 0, len(update.SmfResources))
	for _, association := range update.SmfResources {
		associations = append(associations, nwdaf_context.SmfPeerResourceAssociation{
			TargetAPIBaseURI:     association.TargetAPIBaseURI,
			PeerSubscriptionID:   association.PeerSubscriptionID,
			NwdafSubscriptionIDs: append([]string(nil), association.NwdafSubscriptionIDs...),
		})
	}
	accepted, changed := p.nwdafContext.ReplaceSmfPeerResourceAssociations(associations)
	if !accepted {
		return ErrUnknownSmfResource
	}
	if changed && p.mtlfSyncRefresher != nil {
		p.mtlfSyncRefresher.Refresh()
	}
	return nil
}

func (p *Processor) ReplaceTrainingDataDescriptors(
	update backend.TrainingDataDescriptorUpdate,
) error {
	if p == nil || p.nwdafContext == nil || p.availability == nil {
		return ErrBackendUnavailable
	}
	snapshot := p.availability.Snapshot()
	if snapshot.State != backend.StateUsable {
		return ErrBackendUnavailable
	}
	if update.ProcessInstanceID == "" || update.ProcessInstanceID != snapshot.ProcessInstanceID {
		return ErrStaleBackendProcess
	}
	descriptors := make([]json.RawMessage, 0, len(update.TrainingDataDescriptors))
	for _, descriptor := range update.TrainingDataDescriptors {
		encoded, err := json.Marshal(descriptor)
		if err != nil {
			return err
		}
		descriptors = append(descriptors, encoded)
	}
	if p.nwdafContext.ReplaceTrainingDataDescriptors(descriptors) && p.mtlfSyncRefresher != nil {
		p.mtlfSyncRefresher.Refresh()
	}
	return nil
}
