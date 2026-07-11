package mtlf

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/free5gc/nwdaf/internal/anlf/contract"
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
	store       retrainingState

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
	store retrainingState,
) {
	tid := uuid.New().String()
	notifURI := m.buildCollectorRetrievalNotifyURL()

	nwdafCtx := nwdaf_context.GetSelf()
	seenSupis := map[string]bool{}
	var infos []*nwdaf_context.AdrfSmfInfo
	if value, ok := m.accuracyReportContexts.Load(oldModelUrl); ok {
		report := value.(contract.ModelAccuracyReport)
		for _, sourceID := range report.RetrainContext.ObservationSourceIDs {
			info := nwdafCtx.GetAdrfSmfInfo(sourceID)
			if info == nil || seenSupis[info.Supi] {
				continue
			}
			seenSupis[info.Supi] = true
			infos = append(infos, info)
		}
	} else {
		mtlfLog.Warn("AdrfRetrain: fallback reason=no-retrain-context")
		m.submitDaisyTask(mtlfCfg, "", oldModelUrl, store)
		return
	}

	if len(infos) == 0 {
		mtlfLog.Warn("AdrfRetrain: fallback reason=no-adrf-targets")
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
		mtlfLog.Warnf("AdrfRetrain: watchdog task=%s timeout=%ds", tid, int(watchdogDuration.Seconds()))
		job.closeFetchQueue()
	})

	// Execute RetrievalSubscribes
	adrfClient := m.adrfClient
	if adrfClient == nil {
		mtlfLog.Warnf("AdrfRetrain: fallback task=%s reason=no-client", tid)
		m.activeJobs.Delete(tid)
		job.watchdog.Stop()
		m.submitDaisyTask(mtlfCfg, "", oldModelUrl, store)
		return
	}
	for _, info := range infos {
		subscriptionId, err := adrfClient.RetrievalSubscribe(m.nwdaf.CancelContext(), info, tid, notifURI, timePeriod)
		if err != nil {
			mtlfLog.Warnf("CreateAdrfRetrievalSubscription failed: task=%s err=%v", tid, err)
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
		mtlfLog.Warnf("AdrfRetrain: fallback task=%s reason=subscribe-failed", tid)
		m.activeJobs.Delete(tid)
		job.watchdog.Stop()
		m.submitDaisyTask(mtlfCfg, "", oldModelUrl, store)
		return
	}

	mtlfLog.Infof("AdrfRetrain: subscribed task=%s targets=%d", tid, totalSubs)

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
			mtlfLog.Errorf("FetchRetrainData panic: task=%s err=%v", job.tid, r)
		}
		job.watchdog.Stop()
		m.activeJobs.Delete(job.tid)
	}()

	fetchBatchSize := adrfCfg.FetchBatchSizeOrDefault()
	if m.daisyClient == nil {
		mtlfLog.Errorf("FetchRetrainData failed: task=%s reason=no-daisy-client", job.tid)
		cleanupSubscriptions(job, adrfClient)
		return
	}

	var totalIDs, fetched, uploaded int
	shutdown := false
	cancelCtx := m.nwdaf.CancelContext()
	if cancelCtx == nil {
		mtlfLog.Warnf("FetchRetrainData: skipped task=%s reason=no-app-context", job.tid)
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
			mtlfLog.Infof("FetchRetrainData: stopped task=%s reason=shutdown", job.tid)
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

			mtlfLog.Debugf("FetchRetrainData: request task=%s ids=%d", job.tid, len(chunk))
			record, err := adrfClient.RetrievalRequest(m.nwdaf.CancelContext(), chunk)
			if err != nil {
				mtlfLog.Errorf("FetchRetrainData failed: task=%s err=%v", job.tid, err)
				continue
			}
			if record == nil {
				mtlfLog.Debugf("FetchRetrainData: empty task=%s", job.tid)
				continue
			}

			if record.DataNotif == nil || len(record.DataNotif.UpfEventNotifs) == 0 {
				mtlfLog.Debugf("FetchRetrainData: empty-data task=%s", job.tid)
				continue
			}
			fetched++

			supi := ""
			if len(record.DataSub) > 0 && record.DataSub[0].SmfDataSub != nil {
				supi = record.DataSub[0].SmfDataSub.Supi
			}
			groupId := job.supiToGroup[supi]
			if supi != "" && groupId == "" {
				mtlfLog.Debugf("UploadRetrainData: task=%s group=unresolved", job.tid)
			}

			if uploadErr := m.daisyClient.UploadData(
				m.nwdaf.CancelContext(),
				job.tid,
				groupId,
				record.DataNotif.UpfEventNotifs,
			); uploadErr != nil {
				mtlfLog.Errorf("UploadRetrainData failed: task=%s err=%v", job.tid, uploadErr)
			} else {
				mtlfLog.Debugf("UploadRetrainData: uploaded task=%s records=%d", job.tid, len(record.DataNotif.UpfEventNotifs))
				uploaded++
			}
		}
	}

	mtlfLog.Infof("FetchRetrainData: completed task=%s ids=%d fetched=%d uploaded=%d",
		job.tid, totalIDs, fetched, uploaded)

	// cleanup subscriptions before deciding whether training should continue
	cleanupSubscriptions(job, adrfClient)
	if shutdown {
		mtlfLog.Infof("SubmitTrainingTask: skipped task=%s reason=shutdown", job.tid)
		return
	}
	// activeJobs.Delete is handled by defer above
	m.submitDaisyTask(mtlfCfg, job.tid, job.oldModelUrl, job.store)
}

// HandleAdrfRetrievalNotify routes an ADRF retrieval callback to the matching job.
func (m *MtlfService) HandleAdrfRetrievalNotify(notifCorrId string, fetchCorrIds []string, terminationReq bool) {
	val, ok := m.activeJobs.Load(notifCorrId)
	if !ok {
		mtlfLog.Warnf("HandleAdrfRetrievalNotify: unknown task=%s", notifCorrId)
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

	mtlfLog.Infof("HandleAdrfRetrievalNotify: task=%s ids=%d terminationReq=%t termCount=%d/%d",
		notifCorrId, len(fetchCorrIds), terminationReq, termCount, totalSubs)

	cancelCtx := m.nwdaf.CancelContext()
	if cancelCtx == nil {
		mtlfLog.Warnf("HandleAdrfRetrievalNotify: task=%s reason=no-app-context", notifCorrId)
		return
	}
	if !isClosed && len(fetchCorrIds) > 0 && !job.enqueueFetchIDs(cancelCtx, fetchCorrIds) {
		if cancelCtx.Err() != nil {
			mtlfLog.Infof("HandleAdrfRetrievalNotify: task=%s drop=shutdown", notifCorrId)
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
			mtlfLog.Warnf("DeleteAdrfRetrievalSubscription failed: sub=%s err=%v", subId, err)
		}
	}
}
