package processor

import (
	"context"
	"encoding/json"
	"sync"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/nwdaf/internal/sbi/consumer"
)

// adrfBuffer accumulates UPF notifications per correlationId and flushes them
// to ADRF once the per-UE count reaches the configured threshold.
//
// threshold=1 (default): every notification is forwarded immediately.
// threshold=N: N notifications from the same UE are merged into one ADRF record.
type adrfBuffer struct {
	mu        sync.Mutex
	pending   map[string][]json.RawMessage // correlationId → accumulated notifJSONs
	threshold int
	baseCtx   context.Context
	client    consumer.AdrfServiceAPI
}

func newAdrfBuffer(threshold int, baseCtx context.Context, client consumer.AdrfServiceAPI) *adrfBuffer {
	return &adrfBuffer{
		pending:   make(map[string][]json.RawMessage),
		threshold: threshold,
		baseCtx:   baseCtx,
		client:    client,
	}
}

// add appends one UPF notification JSON to the sub-buffer for its correlationId.
// Flushes the sub-buffer immediately when it reaches the threshold.
func (b *adrfBuffer) add(info *nwdaf_context.AdrfSmfInfo, notifJSON json.RawMessage) {
	b.mu.Lock()
	defer b.mu.Unlock()

	correlationId := info.NotifId
	b.pending[correlationId] = append(b.pending[correlationId], notifJSON)

	if len(b.pending[correlationId]) >= b.threshold {
		b.flushOne(correlationId, info)
	}
}

// flushOne sends all accumulated notifJSONs for one correlationId to ADRF
// and clears that sub-buffer. Must be called under b.mu.
func (b *adrfBuffer) flushOne(correlationId string, info *nwdaf_context.AdrfSmfInfo) {
	notifJSONs := b.pending[correlationId]

	storeTransId, err := b.client.StorageRequest(b.baseCtx, info, notifJSONs)
	if err != nil {
		logger.ProcLog.Warnf("StoreAdrfRecord failed: corr=%s err=%v", correlationId, err)
	} else {
		logger.ProcLog.Infof("StoreAdrfRecord: stored corr=%s store=%s count=%d",
			correlationId, storeTransId, len(notifJSONs))
	}

	delete(b.pending, correlationId)
}
