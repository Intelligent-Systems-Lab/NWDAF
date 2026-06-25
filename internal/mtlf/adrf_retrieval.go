package mtlf

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/sbi/consumer"
	"github.com/free5gc/nwdaf/pkg/factory"
)

const adrfCleanupUnsubscribeTimeout = 5 * time.Second

// retrainJob tracks the state of one ADRF-assisted retrain operation.
// One job per TID; created in runAdrfRetrainWorkflow, destroyed after convergence.
type retrainJob struct {
	tid         string
	oldModelUrl string
	store       *nwdaf_context.ModelAccuracyStore

	totalSubs        int               // expected terminationReq count
	watchdogDuration time.Duration     // computed once at creation; used for Reset calls
	supiToGroup      map[string]string // SUPI → groupId; computed once at creation

	// Protected by mu
	mu              sync.Mutex
	subscriptionIds []string
	termCount       int
	closed          bool // true after fetchCh is closed; guards against late sends

	// Channel-based fetch queue: closed on convergence signal
	fetchCh chan []string

	// closeOnce ensures fetchCh is closed exactly once
	closeOnce sync.Once

	// Watchdog timer: reset on each callback
	watchdog *time.Timer
}

func (j *retrainJob) closeFetchQueue() {
	if j == nil {
		return
	}
	j.closeOnce.Do(func() {
		j.mu.Lock()
		defer j.mu.Unlock()
		if j.closed {
			return
		}
		j.closed = true
		close(j.fetchCh)
	})
}

func (j *retrainJob) enqueueFetchIDs(ctx context.Context, ids []string) bool {
	if j == nil || len(ids) == 0 || ctx == nil {
		return false
	}

	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return false
	}

	select {
	case j.fetchCh <- ids:
		return true
	case <-ctx.Done():
		return false
	}
}

// runAdrfRetrainWorkflow runs the full ADRF retrieval flow for a retrain job.
// When launched from retrain dispatch, this workflow is expected to stay under
// the owning MTLF lifecycle boundary rather than spawning detached follow-up work.
func (m *MtlfService) runAdrfRetrainWorkflow(
	mtlfCfg *factory.MtlfConfig,
	adrfCfg *factory.AdrfConfig,
	oldModelUrl string,
	store *nwdaf_context.ModelAccuracyStore,
) {
	tid := uuid.New().String()
	notifURI := m.buildNwdafURL("/collector/retrieval-notify")

	// Collect AdrfSmfInfos for SUPIs serving this model
	nwdafCtx := nwdaf_context.GetSelf()
	sharedModel := nwdafCtx.GetSharedModel(oldModelUrl)
	if sharedModel == nil {
		mtlfLog.Warnf("runAdrfRetrainWorkflow: no SharedModelInfo for model=%s, fallback to direct training", oldModelUrl)
		m.submitDaisyTask(mtlfCfg, "", oldModelUrl, store)
		return
	}

	subscriberIds := sharedModel.GetSubscriberIDs()
	seenSupis := map[string]bool{}
	var infos []*nwdaf_context.AdrfSmfInfo
	for _, nwdafSubId := range subscriberIds {
		for _, resource := range nwdafCtx.GetNwdafSubResources(nwdafSubId) {
			if seenSupis[resource.Supi] {
				continue
			}
			info := nwdafCtx.GetAdrfSmfInfo(resource.CorrelationId)
			if info == nil {
				continue
			}
			seenSupis[resource.Supi] = true
			infos = append(infos, info)
		}
	}

	if len(infos) == 0 {
		mtlfLog.Warnf("runAdrfRetrainWorkflow: no ADRF-tracked SUPIs for model=%s, fallback to direct training", oldModelUrl)
		m.submitDaisyTask(mtlfCfg, "", oldModelUrl, store)
		return
	}

	// Build SUPI→groupId reverse map once; used per record in runFetchLoop
	resolver := nwdafCtx.GetGroupResolver()
	supiToGroup := make(map[string]string, len(seenSupis))
	for _, gid := range resolver.GetAllGroups() {
		supis, err := resolver.ResolveGroupId(gid)
		if err != nil {
			continue
		}
		for _, s := range supis {
			if seenSupis[s] {
				supiToGroup[s] = gid
			}
		}
	}

	// Build time period
	now := time.Now().UTC()
	retrainWindow := time.Duration(adrfCfg.RetrainWindowOrDefault()) * time.Second
	timePeriod := consumer.AdrfTimePeriod{
		StartTime: now.Add(-retrainWindow).Format(time.RFC3339),
		StopTime:  now.Format(time.RFC3339),
	}

	watchdogDuration := time.Duration(adrfCfg.WatchdogTimeoutOrDefault()) * time.Second

	job := &retrainJob{
		tid:              tid,
		oldModelUrl:      oldModelUrl,
		store:            store,
		totalSubs:        len(infos),
		watchdogDuration: watchdogDuration,
		supiToGroup:      supiToGroup,
		fetchCh:          make(chan []string, len(infos)*4),
	}

	// Register BEFORE subscribing so callbacks during subscribe can be routed
	m.activeJobs.Store(tid, job)

	// Start watchdog immediately after registering
	job.watchdog = time.AfterFunc(watchdogDuration, func() {
		mtlfLog.Warnf("ADRF watchdog fired for TID=%s: no terminationReq after %ds, proceeding with available data",
			tid, int(watchdogDuration.Seconds()))
		job.closeFetchQueue()
	})

	// Execute RetrievalSubscribes
	adrfClient := m.adrfClient
	if adrfClient == nil {
		mtlfLog.Warnf("runAdrfRetrainWorkflow: ADRF client not initialized for TID=%s, fallback to direct training", tid)
		m.activeJobs.Delete(tid)
		job.watchdog.Stop()
		m.submitDaisyTask(mtlfCfg, "", oldModelUrl, store)
		return
	}
	for _, info := range infos {
		subscriptionId, err := adrfClient.RetrievalSubscribe(m.nwdaf.CancelContext(), info, tid, notifURI, timePeriod)
		if err != nil {
			mtlfLog.Warnf("runAdrfRetrainWorkflow: RetrievalSubscribe failed for supi=%s: %v", info.Supi, err)
			job.mu.Lock()
			job.totalSubs--
			shouldConverge := job.totalSubs > 0 && job.termCount >= job.totalSubs
			job.mu.Unlock()
			if shouldConverge {
				job.closeFetchQueue()
			}
			continue
		}
		job.mu.Lock()
		job.subscriptionIds = append(job.subscriptionIds, subscriptionId)
		job.mu.Unlock()
	}

	// Check if all subscribes failed
	job.mu.Lock()
	totalSubs := job.totalSubs
	job.mu.Unlock()
	if totalSubs == 0 {
		mtlfLog.Warnf("runAdrfRetrainWorkflow: all ADRF RetrievalSubscribes failed for TID=%s", tid)
		m.activeJobs.Delete(tid)
		job.watchdog.Stop()
		m.submitDaisyTask(mtlfCfg, "", oldModelUrl, store)
		return
	}

	m.runFetchLoop(job, adrfCfg, mtlfCfg, adrfClient)
}

// runFetchLoop drains fetchCh: for each batch of IDs, fetches records from ADRF
// and uploads to Daisy. Exits when fetchCh is closed or app shutdown begins.
func (m *MtlfService) runFetchLoop(
	job *retrainJob,
	adrfCfg *factory.AdrfConfig,
	mtlfCfg *factory.MtlfConfig,
	adrfClient consumer.AdrfServiceAPI,
) {
	defer func() {
		if r := recover(); r != nil {
			mtlfLog.Errorf("runFetchLoop TID=%s panicked: %v", job.tid, r)
		}
		job.watchdog.Stop()
		m.activeJobs.Delete(job.tid)
	}()

	fetchBatchSize := adrfCfg.FetchBatchSizeOrDefault()
	if m.daisyClient == nil {
		mtlfLog.Errorf("runFetchLoop TID=%s: Daisy client not initialized", job.tid)
		cleanupSubscriptions(job, adrfClient)
		return
	}

	var totalIDs, fetched, uploaded int
	shutdown := false
	cancelCtx := m.nwdaf.CancelContext()
	if cancelCtx == nil {
		mtlfLog.Warnf("runFetchLoop TID=%s: missing app cancel context", job.tid)
		cleanupSubscriptions(job, adrfClient)
		return
	}

loop:
	for {
		var ids []string
		var ok bool

		select {
		case <-cancelCtx.Done():
			shutdown = true
			job.closeFetchQueue()
			mtlfLog.Infof("runFetchLoop TID=%s stopping due to app shutdown", job.tid)
			break loop
		case ids, ok = <-job.fetchCh:
			if !ok {
				break loop
			}
		}

		totalIDs += len(ids)
		for i := 0; i < len(ids); i += fetchBatchSize {
			end := i + fetchBatchSize
			if end > len(ids) {
				end = len(ids)
			}
			chunk := ids[i:end]

			mtlfLog.Debugf("runFetchLoop TID=%s: fetching id=%s", job.tid, chunk[0])
			record, err := adrfClient.RetrievalRequest(m.nwdaf.CancelContext(), chunk)
			if err != nil {
				mtlfLog.Errorf("runFetchLoop TID=%s: RetrievalRequest failed: %v", job.tid, err)
				continue
			}
			if record == nil {
				mtlfLog.Debugf("runFetchLoop TID=%s: id=%s no data (204)", job.tid, chunk[0])
				continue
			}

			if record.DataNotif == nil || len(record.DataNotif.UpfEventNotifs) == 0 {
				mtlfLog.Debugf("runFetchLoop TID=%s: id=%s empty dataNotif", job.tid, chunk[0])
				continue
			}
			fetched++

			supi := ""
			if len(record.DataSub) > 0 && record.DataSub[0].SmfDataSub != nil {
				supi = record.DataSub[0].SmfDataSub.Supi
			}
			groupId := job.supiToGroup[supi]
			if supi != "" && groupId == "" {
				mtlfLog.Warnf("runFetchLoop TID=%s: SUPI %s not in any group, uploading with empty groupId", job.tid, supi)
			}

			if uploadErr := m.daisyClient.UploadData(
				m.nwdaf.CancelContext(),
				job.tid,
				groupId,
				record.DataNotif.UpfEventNotifs,
			); uploadErr != nil {
				mtlfLog.Errorf("runFetchLoop TID=%s: UploadData failed: %v", job.tid, uploadErr)
			} else {
				mtlfLog.Debugf("runFetchLoop TID=%s: uploaded id=%s supi=%s groupId=%s", job.tid, chunk[0], supi, groupId)
				uploaded++
			}
		}
	}

	mtlfLog.Infof("runFetchLoop TID=%s complete: ids=%d fetched=%d uploaded=%d",
		job.tid, totalIDs, fetched, uploaded)

	// cleanup subscriptions before deciding whether training should continue
	cleanupSubscriptions(job, adrfClient)
	if shutdown {
		mtlfLog.Infof("runFetchLoop TID=%s: skipping Daisy dispatch during shutdown", job.tid)
		return
	}
	// activeJobs.Delete is handled by defer above
	m.submitDaisyTask(mtlfCfg, job.tid, job.oldModelUrl, job.store)
}

// HandleAdrfRetrievalNotify routes an ADRF retrieval callback to the matching job.
func (m *MtlfService) HandleAdrfRetrievalNotify(notifCorrId string, fetchCorrIds []string, terminationReq bool) {
	val, ok := m.activeJobs.Load(notifCorrId)
	if !ok {
		mtlfLog.Warnf("HandleAdrfRetrievalNotify: unknown notifCorrId=%s", notifCorrId)
		return
	}
	job := val.(*retrainJob)

	job.mu.Lock()
	if job.watchdog != nil {
		job.watchdog.Reset(job.watchdogDuration)
	}
	if terminationReq {
		job.termCount++
	}
	allTermReceived := job.termCount >= job.totalSubs && job.totalSubs > 0
	isClosed := job.closed
	termCount, totalSubs := job.termCount, job.totalSubs
	job.mu.Unlock()

	mtlfLog.Infof("RetrievalNotify TID=%s: ids=%d terminationReq=%t termCount=%d/%d",
		notifCorrId, len(fetchCorrIds), terminationReq, termCount, totalSubs)

	cancelCtx := m.nwdaf.CancelContext()
	if cancelCtx == nil {
		mtlfLog.Warnf("RetrievalNotify TID=%s: missing app cancel context", notifCorrId)
		return
	}
	if !isClosed && len(fetchCorrIds) > 0 && !job.enqueueFetchIDs(cancelCtx, fetchCorrIds) {
		if cancelCtx.Err() != nil {
			mtlfLog.Infof("RetrievalNotify TID=%s: dropping ids during shutdown", notifCorrId)
		}
	}

	if allTermReceived {
		job.closeFetchQueue()
	}
}

// cleanupSubscriptions sends DELETE for all registered subscriptionIds.
func cleanupSubscriptions(job *retrainJob, client consumer.AdrfServiceAPI) {
	if job == nil || client == nil {
		return
	}

	job.mu.Lock()
	subIds := make([]string, len(job.subscriptionIds))
	copy(subIds, job.subscriptionIds)
	job.mu.Unlock()

	for _, subId := range subIds {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), adrfCleanupUnsubscribeTimeout)
		err := client.RetrievalUnsubscribe(cleanupCtx, subId)
		cancel()
		if err != nil {
			mtlfLog.Warnf("cleanupSubscriptions: failed to unsubscribe %s: %v", subId, err)
		}
	}
}
