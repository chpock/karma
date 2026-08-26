package main

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/prymitive/karma/internal/fork/alertlifecycle"
	runtimeLog "github.com/prymitive/karma/internal/log"
)

const recheckSchedulerBatchSize = 1000

type recheckSchedulerTimer interface {
	Chan() <-chan time.Time
	Stop() bool
	Reset(time.Duration) bool
}

type recheckSchedulerClock interface {
	Now() time.Time
	NewTimer(time.Duration) recheckSchedulerTimer
}

type realRecheckSchedulerClock struct{}

type realRecheckSchedulerTimer struct{ timer *time.Timer }

type recheckSchedulerBackoffOwner string

type recheckSchedulerTimerKind uint8

const (
	recheckSchedulerTimerNone recheckSchedulerTimerKind = iota
	recheckSchedulerTimerBackoff
	recheckSchedulerTimerPersistedDue
)

const (
	recheckBackoffAcquisition    recheckSchedulerBackoffOwner = "acquisition"
	recheckBackoffPreAttempt     recheckSchedulerBackoffOwner = "pre_attempt"
	recheckBackoffBookkeeping    recheckSchedulerBackoffOwner = "bookkeeping"
	recheckBackoffReconciliation recheckSchedulerBackoffOwner = "reconciliation"
	recheckBackoffQueueRead      recheckSchedulerBackoffOwner = "queue_read"
)

func (realRecheckSchedulerClock) Now() time.Time { return time.Now().UTC() }

func (realRecheckSchedulerClock) NewTimer(delay time.Duration) recheckSchedulerTimer {
	return &realRecheckSchedulerTimer{timer: time.NewTimer(delay)}
}

func (t *realRecheckSchedulerTimer) Chan() <-chan time.Time { return t.timer.C }

func (t *realRecheckSchedulerTimer) Stop() bool { return t.timer.Stop() }

func (t *realRecheckSchedulerTimer) Reset(delay time.Duration) bool { return t.timer.Reset(delay) }

type recheckScheduler struct {
	store                    *alertlifecycle.Store
	clock                    recheckSchedulerClock
	wake                     chan struct{}
	ctx                      context.Context
	cancel                   context.CancelFunc
	done                     chan struct{}
	runner                   func(context.Context, int) (alertlifecycle.RecheckBatchResult, error)
	head                     func(context.Context) (alertlifecycle.RecheckQueueHead, error)
	startOnce                sync.Once
	stopOnce                 sync.Once
	stopWaitOnce             sync.Once
	doneOnce                 sync.Once
	stateMu                  sync.Mutex
	processing               bool
	wakePending              bool
	reconciledWakePending    bool
	terminal                 bool
	timerActive              bool
	timerKind                recheckSchedulerTimerKind
	timerOwner               recheckSchedulerBackoffOwner
	timerAt                  time.Time
	backoffStep              map[recheckSchedulerBackoffOwner]int
	acquisitionFailureActive bool
	blockedHeadID            int64
	blockedHeadStatus        string
	// Test-only causal barriers. They are nil on every production scheduler.
	afterProcess     func()
	wakeDeferred     func()
	signalDeferred   func()
	stopWaitBoundary func()
	idleLoopProbe    chan struct{}
}

type recheckSchedulerProcessResult struct {
	timerC   <-chan time.Time
	terminal bool
}

func newRecheckScheduler(store *alertlifecycle.Store, clock recheckSchedulerClock) *recheckScheduler {
	if clock == nil {
		clock = realRecheckSchedulerClock{}
	}
	ctx, cancel := context.WithCancel(context.Background())
	scheduler := &recheckScheduler{store: store, clock: clock, wake: make(chan struct{}, 1), ctx: ctx, cancel: cancel, done: make(chan struct{}), backoffStep: make(map[recheckSchedulerBackoffOwner]int)}
	scheduler.runner = func(ctx context.Context, batchSize int) (alertlifecycle.RecheckBatchResult, error) {
		if store == nil {
			return alertlifecycle.RecheckBatchResult{Disposition: alertlifecycle.RecheckBatchAcquisitionFailed}, alertlifecycle.ErrStoreNotInitialized
		}
		return store.RunSilenceRecheckBatchContext(ctx, batchSize)
	}
	scheduler.head = func(ctx context.Context) (alertlifecycle.RecheckQueueHead, error) {
		if store == nil {
			return alertlifecycle.RecheckQueueHead{}, alertlifecycle.ErrStoreNotInitialized
		}
		return store.RecheckQueueHead(ctx)
	}
	return scheduler
}

func (s *recheckScheduler) Start() {
	if s == nil {
		return
	}
	s.startOnce.Do(func() {
		go s.run()
		s.Signal()
	})
}

func (s *recheckScheduler) Signal() {
	if s == nil {
		return
	}
	s.stateMu.Lock()
	if s.terminal {
		s.stateMu.Unlock()
		return
	}
	s.wakePending = true
	timerActive := s.timerActive
	processing := s.processing
	shouldQueue := !processing && !timerActive
	deferredQueue := !processing && timerActive
	s.stateMu.Unlock()
	if timerActive && s.signalDeferred != nil {
		s.signalDeferred()
	}
	if !shouldQueue && !deferredQueue {
		return
	}
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *recheckScheduler) SignalReconciled() {
	if s == nil {
		return
	}
	s.stateMu.Lock()
	if s.terminal {
		s.stateMu.Unlock()
		return
	}
	s.reconciledWakePending = true
	shouldQueue := !s.processing && !s.timerActive
	deferredQueue := !s.processing && s.timerActive
	s.stateMu.Unlock()
	if !shouldQueue && !deferredQueue {
		return
	}
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *recheckScheduler) Stop() {
	if s == nil {
		return
	}
	s.stopOnce.Do(func() {
		s.cancel()
		s.startOnce.Do(func() { s.closeDone() })
	})
	if s.stopWaitBoundary != nil {
		s.stopWaitOnce.Do(s.stopWaitBoundary)
	}
	<-s.done
}

func (s *recheckScheduler) closeDone() {
	s.doneOnce.Do(func() { close(s.done) })
}

func (s *recheckScheduler) beginProcess(timerFired bool) bool {
	s.stateMu.Lock()
	if s.terminal || s.ctx.Err() != nil || s.processing {
		s.stateMu.Unlock()
		return false
	}
	if timerFired {
		if !s.timerActive {
			s.stateMu.Unlock()
			return false
		}
		s.timerActive = false
		s.timerKind = recheckSchedulerTimerNone
		s.timerOwner = ""
		s.timerAt = time.Time{}
		s.wakePending = false
		s.reconciledWakePending = false
	} else {
		if s.timerActive {
			deferred := s.wakeDeferred
			s.stateMu.Unlock()
			if deferred != nil {
				deferred()
			}
			return false
		}
		s.wakePending = false
		s.reconciledWakePending = false
	}
	s.processing = true
	s.stateMu.Unlock()
	return true
}

func (s *recheckScheduler) finishProcess(result recheckSchedulerProcessResult) {
	s.stateMu.Lock()
	s.processing = false
	if result.terminal {
		s.terminal = true
		s.wakePending = false
		s.reconciledWakePending = false
	}
	shouldQueue := !result.terminal && !s.timerActive && (s.wakePending || s.reconciledWakePending)
	if shouldQueue {
		s.wakePending = false
		s.reconciledWakePending = false
	}
	s.stateMu.Unlock()
	if shouldQueue {
		select {
		case s.wake <- struct{}{}:
		default:
		}
	}
	if s.afterProcess != nil {
		s.afterProcess()
	}
}

func (s *recheckScheduler) run() {
	defer func() {
		s.closeDone()
	}()
	timer := s.clock.NewTimer(24 * time.Hour)
	defer timer.Stop()
	timer.Stop()
	var timerC <-chan time.Time
	for {
		s.acknowledgeIdleLoopProbe()
		select {
		case <-s.ctx.Done():
			return
		case <-s.wake:
			if !s.beginProcess(false) {
				continue
			}
			result := s.process(timer, nil)
			s.finishProcess(result)
			if result.terminal {
				return
			}
			timerC = result.timerC
		case <-timerC:
			if !s.beginProcess(true) {
				continue
			}
			result := s.process(timer, nil)
			s.finishProcess(result)
			if result.terminal {
				return
			}
			timerC = result.timerC
		}
	}
}

func (s *recheckScheduler) acknowledgeIdleLoopProbe() {
	s.stateMu.Lock()
	probe := s.idleLoopProbe
	s.idleLoopProbe = nil
	s.stateMu.Unlock()
	if probe != nil {
		close(probe)
	}
}

func (s *recheckScheduler) process(timer recheckSchedulerTimer, timerC <-chan time.Time) recheckSchedulerProcessResult {
	if timerC != nil {
		s.stateMu.Lock()
		s.timerActive = false
		s.timerKind = recheckSchedulerTimerNone
		s.timerOwner = ""
		s.timerAt = time.Time{}
		s.stateMu.Unlock()
	}
	for {
		if s.ctx.Err() != nil {
			return recheckSchedulerProcessResult{terminal: true}
		}
		batchResult, err := s.runner(s.ctx, recheckSchedulerBatchSize)
		s.resetAfterReconciledResult(batchResult.ReconciledOperation)
		if err == nil {
			switch batchResult.Disposition {
			case alertlifecycle.RecheckBatchQueueBlocked:
				s.resetAfterSelectionSuccess()
				s.logBestEffort(func() { s.logRecheckQueueBlocked(batchResult) })
				return recheckSchedulerProcessResult{}
			case alertlifecycle.RecheckBatchRetryDeferred:
				s.resetAfterSelectionSuccess()
				s.logBestEffort(func() { s.logRecheckRetryDeferred(batchResult, batchResult.NextAttemptAt) })
				if batchResult.NextAttemptAt != nil {
					return recheckSchedulerProcessResult{timerC: s.resetPersistedTimer(timer, *batchResult.NextAttemptAt)}
				}
				return recheckSchedulerProcessResult{}
			case alertlifecycle.RecheckBatchQueueEmpty,
				alertlifecycle.RecheckBatchCommitted,
				alertlifecycle.RecheckBatchRetryScheduled,
				alertlifecycle.RecheckBatchAcquisitionFailed,
				alertlifecycle.RecheckBatchInfrastructureFailed,
				alertlifecycle.RecheckBatchCommitOutcomeUnknown,
				alertlifecycle.RecheckBatchJobFailed,
				alertlifecycle.RecheckBatchFailurePersistenceFailed:
				// These dispositions are paired with their operational error below.
			default:
				// Other dispositions are paired with their operational error below.
			}
		}
		if batchResult.Disposition == alertlifecycle.RecheckBatchRetryScheduled && batchResult.NextAttemptAt != nil {
			s.resetAfterBookkeepingSuccess()
			s.clearBlockedHeadEdge()
			s.logBestEffort(func() { s.logRecheckRetryScheduled(batchResult, *batchResult.NextAttemptAt) })
			return recheckSchedulerProcessResult{timerC: s.resetPersistedTimer(timer, *batchResult.NextAttemptAt)}
		}
		if err != nil {
			if batchResult.Disposition == alertlifecycle.RecheckBatchInfrastructureFailed {
				s.clearBlockedHeadEdge()
				s.logBestEffort(func() {
					s.logRecheckInfrastructure(batchResult, batchResult.FailureClass, batchResult.FailureStage, err)
				})
				return recheckSchedulerProcessResult{terminal: true}
			}
			if errors.Is(err, alertlifecycle.ErrRecheckFailurePersistence) && !errors.Is(err, alertlifecycle.ErrWriterCommitOutcomeUnknown) && !errors.Is(err, alertlifecycle.ErrWriterCommitReconciled) && !alertlifecycle.IsRetryableRecheckFailurePersistence(err) {
				s.logBestEffort(func() {
					s.logRecheckInfrastructure(batchResult, failurePersistenceReason(err), "failure_persistence", err)
				})
				return recheckSchedulerProcessResult{terminal: true}
			}
			if batchResult.Disposition == alertlifecycle.RecheckBatchJobFailed || alertlifecycle.IsRecheckDurableFailure(err) {
				s.resetAfterBookkeepingSuccess()
				s.clearBlockedHeadEdge()
				return recheckSchedulerProcessResult{}
			}
			if errors.Is(err, alertlifecycle.ErrWriterCommitReconciled) {
				operation, _, _ := alertlifecycle.RecheckWriterCommitDisposition(err)
				switch operation {
				case alertlifecycle.RecheckBatchWriterOperation:
					s.resetAfterReconciliationSuccess()
				case alertlifecycle.RecheckFailureWriterOperation:
					s.resetAfterFailureReconciliationSuccess()
				}
				if operation == alertlifecycle.RecheckBatchWriterOperation || operation == alertlifecycle.RecheckFailureWriterOperation {
					s.stateMu.Lock()
					s.reconciledWakePending = false
					s.stateMu.Unlock()
				}
				continue
			}
			var retryScheduled interface{ NextTime() time.Time }
			if errors.As(err, &retryScheduled) {
				s.resetAfterBookkeepingSuccess()
				return recheckSchedulerProcessResult{timerC: s.resetPersistedTimer(timer, retryScheduled.NextTime())}
			}
			var queueBlocked interface{ QueueBlocked() (string, *time.Time) }
			if errors.As(err, &queueBlocked) {
				s.resetAfterSelectionSuccess()
				status, next := queueBlocked.QueueBlocked()
				if status == "retry_wait" {
					s.logBestEffort(func() { s.logRecheckRetryDeferred(batchResult, next) })
				} else {
					s.logBestEffort(func() { s.logRecheckQueueBlocked(batchResult) })
				}
				if status == "retry_wait" && next != nil {
					return recheckSchedulerProcessResult{timerC: s.resetPersistedTimer(timer, *next)}
				}
				return recheckSchedulerProcessResult{}
			}
			if errors.Is(err, alertlifecycle.ErrWriterCommitOutcomeUnknown) {
				operation, _, _ := alertlifecycle.RecheckWriterCommitDisposition(err)
				if operation == alertlifecycle.RecheckBatchWriterOperation || operation == alertlifecycle.RecheckFailureWriterOperation {
					s.resetAfterPreAttemptSuccess()
				}
				if alertlifecycle.RecheckFailurePersistenceReconciliationResolved(err) {
					s.resetBackoff(recheckBackoffReconciliation)
				}
				return recheckSchedulerProcessResult{timerC: s.scheduleBackoff(timer, recheckBackoffReconciliation)}
			}
			if s.ctx.Err() != nil || context.Cause(s.ctx) != nil {
				return recheckSchedulerProcessResult{terminal: true}
			}
			if errors.Is(err, alertlifecycle.ErrRecheckFailurePersistence) {
				if errors.Is(err, alertlifecycle.ErrWriterAcquisition) {
					if !alertlifecycle.IsRetryableRecheckWriterAcquisition(err) {
						s.logBestEffort(func() {
							s.logRecheckInfrastructure(batchResult, "failure_persistence_writer_unavailable", "failure_persistence", err)
						})
						return recheckSchedulerProcessResult{terminal: true}
					}
					s.resetAfterPreAttemptSuccess()
					if alertlifecycle.RecheckFailurePersistenceReconciliationResolved(err) {
						s.resetBackoff(recheckBackoffReconciliation)
					}
					return recheckSchedulerProcessResult{timerC: s.scheduleBackoff(timer, recheckBackoffAcquisition)}
				}
				if !alertlifecycle.IsRetryableRecheckFailurePersistence(err) {
					s.logBestEffort(func() {
						s.logRecheckInfrastructure(batchResult, failurePersistenceReason(err), "failure_persistence", err)
					})
					return recheckSchedulerProcessResult{terminal: true}
				}
				s.resetAfterPreAttemptSuccess()
				if alertlifecycle.RecheckFailurePersistenceReconciliationResolved(err) {
					s.resetBackoff(recheckBackoffReconciliation)
				}
				return recheckSchedulerProcessResult{timerC: s.scheduleBackoff(timer, recheckBackoffBookkeeping)}
			}
			preselectionAcquisition := batchResult.Disposition == alertlifecycle.RecheckBatchAcquisitionFailed && batchResult.JobID == 0 && alertlifecycle.IsWriterAcquisitionError(err)
			if preselectionAcquisition {
				s.logBestEffort(func() { s.logRecheckAcquisitionFailure(batchResult, err) })
			}
			if alertlifecycle.IsRetryableRecheckWriterAcquisition(err) {
				return recheckSchedulerProcessResult{timerC: s.scheduleBackoff(timer, recheckBackoffAcquisition)}
			}
			if alertlifecycle.IsWriterAcquisitionError(err) {
				if !preselectionAcquisition {
					s.logBestEffort(func() { s.logRecheckInfrastructure(batchResult, "writer_unavailable", "batch", err) })
				}
				return recheckSchedulerProcessResult{terminal: true}
			}
			if errors.Is(err, alertlifecycle.ErrWriterOperationInvariant) {
				s.logBestEffort(func() { s.logRecheckInfrastructure(batchResult, "writer_invariant", "batch", err) })
				return recheckSchedulerProcessResult{terminal: true}
			}
			if alertlifecycle.IsRetryableRecheckPreAttempt(err) {
				s.resetAfterAcquisitionSuccess()
				return recheckSchedulerProcessResult{timerC: s.scheduleBackoff(timer, recheckBackoffPreAttempt)}
			}
			if alertlifecycle.IsRecheckPreAttemptError(err) || errors.Is(err, alertlifecycle.ErrWriterOperationToken) {
				s.logBestEffort(func() { s.logRecheckInfrastructure(batchResult, "writer_invariant", "batch", err) })
				return recheckSchedulerProcessResult{terminal: true}
			}
			s.logBestEffort(func() { s.logRecheckInfrastructure(batchResult, "infrastructure_unclassified", "batch", err) })
			return recheckSchedulerProcessResult{terminal: true}
		}
		s.resetAfterRunnerSuccess()
		head, headErr := s.head(s.ctx)
		if headErr != nil {
			if s.ctx.Err() != nil || context.Cause(s.ctx) != nil {
				return recheckSchedulerProcessResult{terminal: true}
			}
			if errors.Is(headErr, alertlifecycle.ErrWriterOperationInvariant) {
				s.logBestEffort(func() { s.logRecheckInfrastructure(batchResult, "writer_invariant", "batch", headErr) })
				return recheckSchedulerProcessResult{terminal: true}
			}
			if alertlifecycle.IsRetryableRecheckStorageError(headErr) {
				return recheckSchedulerProcessResult{timerC: s.scheduleBackoff(timer, recheckBackoffQueueRead)}
			}
			s.logBestEffort(func() { s.logRecheckInfrastructure(batchResult, "queue_read_terminal", "batch", headErr) })
			return recheckSchedulerProcessResult{terminal: true}
		}
		s.resetBackoff(recheckBackoffQueueRead)
		if head.JobID == 0 {
			s.stateMu.Lock()
			s.blockedHeadID = 0
			s.blockedHeadStatus = ""
			s.stateMu.Unlock()
			return recheckSchedulerProcessResult{}
		}
		if head.Status == "failed" {
			queueDepth := head.QueueDepth
			s.logBestEffort(func() {
				s.logRecheckQueueBlocked(alertlifecycle.RecheckBatchResult{JobID: head.JobID, Status: head.Status, QueueDepth: &queueDepth})
			})
			return recheckSchedulerProcessResult{}
		}
		if head.Status == "retry_wait" && head.NextAttemptAt != nil && head.NextAttemptAt.After(s.clock.Now()) {
			return recheckSchedulerProcessResult{timerC: s.resetTimer(timer, *head.NextAttemptAt)}
		}
	}
}

func failurePersistenceReason(err error) string {
	return alertlifecycle.RecheckFailurePersistenceReason(err)
}

func (s *recheckScheduler) resetTimer(timer recheckSchedulerTimer, at time.Time) <-chan time.Time {
	return s.resetPersistedTimer(timer, at)
}

func (s *recheckScheduler) armTimer(kind recheckSchedulerTimerKind, owner recheckSchedulerBackoffOwner, at time.Time) {
	s.stateMu.Lock()
	s.timerActive = true
	s.timerKind = kind
	s.timerOwner = owner
	s.timerAt = at
	s.stateMu.Unlock()
}

func (s *recheckScheduler) resetPersistedTimer(timer recheckSchedulerTimer, at time.Time) <-chan time.Time {
	s.armTimer(recheckSchedulerTimerPersistedDue, "", at)
	return s.resetTimerAfter(timer, at.Sub(s.clock.Now()), s.clock.Now())
}

func (s *recheckScheduler) resetTimerAfter(timer recheckSchedulerTimer, delay time.Duration, _ time.Time) <-chan time.Time {
	if delay < 0 {
		delay = 0
	}
	timer.Stop()
	timer.Reset(delay)
	return timer.Chan()
}

func recheckSchedulerBackoffDuration(step int) time.Duration {
	switch step {
	case 1:
		return time.Second
	case 2:
		return 5 * time.Second
	default:
		return 30 * time.Second
	}
}

func (s *recheckScheduler) scheduleBackoff(timer recheckSchedulerTimer, owner recheckSchedulerBackoffOwner) <-chan time.Time {
	s.backoffStep[owner]++
	at := s.clock.Now().Add(recheckSchedulerBackoffDuration(s.backoffStep[owner]))
	s.armTimer(recheckSchedulerTimerBackoff, owner, at)
	return s.resetTimerAfter(timer, recheckSchedulerBackoffDuration(s.backoffStep[owner]), s.clock.Now())
}

func (s *recheckScheduler) resetBackoff(owner recheckSchedulerBackoffOwner) {
	delete(s.backoffStep, owner)
}

func (s *recheckScheduler) resetAfterPreAttemptSuccess() {
	s.resetBackoff(recheckBackoffAcquisition)
	s.resetBackoff(recheckBackoffPreAttempt)
	s.stateMu.Lock()
	s.acquisitionFailureActive = false
	s.stateMu.Unlock()
}

func (s *recheckScheduler) resetAfterAcquisitionSuccess() {
	s.resetBackoff(recheckBackoffAcquisition)
	s.stateMu.Lock()
	s.acquisitionFailureActive = false
	s.stateMu.Unlock()
}

func (s *recheckScheduler) resetAfterSelectionSuccess() {
	s.resetAfterPreAttemptSuccess()
	s.resetBackoff(recheckBackoffBookkeeping)
	s.resetBackoff(recheckBackoffReconciliation)
	s.stateMu.Lock()
	s.acquisitionFailureActive = false
	s.stateMu.Unlock()
}

func (s *recheckScheduler) clearBlockedHeadEdge() {
	s.stateMu.Lock()
	s.blockedHeadID = 0
	s.blockedHeadStatus = ""
	s.stateMu.Unlock()
}

func (s *recheckScheduler) resetAfterReconciliationSuccess() {
	s.resetAfterPreAttemptSuccess()
	s.resetBackoff(recheckBackoffReconciliation)
}

func (s *recheckScheduler) resetAfterFailureReconciliationSuccess() {
	s.resetAfterPreAttemptSuccess()
	s.resetBackoff(recheckBackoffBookkeeping)
	s.resetBackoff(recheckBackoffReconciliation)
}

func (s *recheckScheduler) resetAfterReconciledResult(operation string) {
	switch operation {
	case alertlifecycle.RecheckBatchWriterOperation:
		s.resetAfterReconciliationSuccess()
	case alertlifecycle.RecheckFailureWriterOperation:
		s.resetAfterFailureReconciliationSuccess()
	}
}

func (s *recheckScheduler) resetAfterBookkeepingSuccess() {
	s.resetAfterSelectionSuccess()
	s.resetBackoff(recheckBackoffBookkeeping)
}

func (s *recheckScheduler) resetAfterRunnerSuccess() {
	s.resetAfterBookkeepingSuccess()
	s.stateMu.Lock()
	s.acquisitionFailureActive = false
	s.stateMu.Unlock()
	s.clearBlockedHeadEdge()
}

func (s *recheckScheduler) logRecheckAcquisitionFailure(result alertlifecycle.RecheckBatchResult, err error) {
	s.stateMu.Lock()
	edge := !s.acquisitionFailureActive
	s.acquisitionFailureActive = true
	s.stateMu.Unlock()
	level := slog.LevelDebug
	if edge {
		level = slog.LevelWarn
	}
	stage := result.FailureStage
	if stage == "" {
		stage = "acquire_writer"
	}
	reasonCode := result.FailureClass
	if reasonCode == "" {
		reasonCode = "writer_acquisition"
	}
	attrs := append(recheckQueueDepthAttrs(result.QueueDepth), slog.Int64("job_id", result.JobID), slog.String("stage", stage), slog.String("reason_code", reasonCode), slog.String("error", alertlifecycle.SanitizeRecheckError(err)))
	runtimeLog.Event(s.ctx, level, "silence_recheck", "worker_acquisition_failed", "Recheck worker could not acquire or prepare its transaction", attrs...)
}

func (s *recheckScheduler) logRecheckRetryDeferred(result alertlifecycle.RecheckBatchResult, next *time.Time) {
	attrs := append(recheckQueueDepthAttrs(result.QueueDepth), slog.Int64("job_id", result.JobID), slog.String("status", "retry_wait"))
	if next != nil {
		attrs = append(attrs, slog.String("next_attempt_at", next.UTC().Format(time.RFC3339Nano)))
	}
	runtimeLog.Event(s.ctx, slog.LevelDebug, "silence_recheck", "retry_deferred", "Recheck retry head is not due", attrs...)
}

func (s *recheckScheduler) logBestEffort(emit func()) {
	failed := false
	func() {
		defer func() {
			if recover() != nil {
				failed = true
			}
		}()
		emit()
	}()
	if !failed {
		return
	}
	defer func() {
		_ = recover()
	}()
	runtimeLog.Event(s.ctx, slog.LevelWarn, "silence_recheck", "observability_collection_failed", "Recheck scheduler event emission failed", slog.String("stage", "scheduler_event"), slog.String("event_family", "scheduler"), slog.String("error", "observability_error"))
}

func (s *recheckScheduler) logRecheckRetryScheduled(result alertlifecycle.RecheckBatchResult, next time.Time) {
	runtimeLog.Event(s.ctx, slog.LevelWarn, "silence_recheck", "retry_scheduled", "Recheck retry was durably scheduled", slog.Int64("job_id", result.JobID), slog.String("status", result.Status), slog.Int("attempt_count", result.AttemptCount), slog.Int("retry_count", result.RetryCount), slog.Int("consecutive_retry_count", result.ConsecutiveRetryCount), slog.String("failure_class", result.FailureClass), slog.String("failure_stage", result.FailureStage), slog.String("next_attempt_at", next.UTC().Format(time.RFC3339Nano)))
}

func (s *recheckScheduler) logRecheckQueueBlocked(result alertlifecycle.RecheckBatchResult) {
	s.stateMu.Lock()
	edge := result.JobID != s.blockedHeadID || result.Status != s.blockedHeadStatus
	s.blockedHeadID = result.JobID
	s.blockedHeadStatus = result.Status
	s.stateMu.Unlock()
	level := slog.LevelDebug
	if edge {
		level = slog.LevelWarn
	}
	attrs := append(recheckQueueDepthAttrs(result.QueueDepth), slog.Int64("job_id", result.JobID), slog.String("status", result.Status))
	runtimeLog.Event(s.ctx, level, "silence_recheck", "failed_job_blocks_queue", "Recheck queue head blocks later jobs", attrs...)
}

func recheckQueueDepthAttrs(queueDepth *int) []slog.Attr {
	attrs := []slog.Attr{slog.Bool("queue_depth_available", queueDepth != nil)}
	if queueDepth != nil {
		attrs = append(attrs, slog.Int("queue_depth", *queueDepth))
	}
	return attrs
}

func (s *recheckScheduler) logRecheckInfrastructure(result alertlifecycle.RecheckBatchResult, reason, stage string, err error) {
	if !alertlifecycle.ValidRecheckInfrastructurePair(stage, reason) {
		runtimeLog.ErrorContext(s.ctx, "silence_recheck", "job_infrastructure_contract_violation", "Invalid silence recheck infrastructure mapping", slog.Int64("job_id", result.JobID), slog.String("stage", stage), slog.String("reason_code", reason))
		return
	}
	level := alertlifecycle.RecheckInfrastructureLogLevel(stage, reason)
	operation := "recheck_batch"
	if result.Disposition == alertlifecycle.RecheckBatchFailurePersistenceFailed || errors.Is(err, alertlifecycle.ErrRecheckFailurePersistence) {
		operation = "recheck_failure"
	}
	runtimeLog.Event(s.ctx, level, "silence_recheck", "job_infrastructure_blocked", "Recheck infrastructure blocked progress", slog.String("writer_operation", operation), slog.String("stage", stage), slog.Int64("job_id", result.JobID), slog.String("reason_code", reason), slog.String("error", alertlifecycle.SanitizeRecheckError(err)))
}

func signalAlertLifecycleRecheck() {
	if lifecycleRecheckScheduler != nil {
		lifecycleRecheckScheduler.Signal()
	}
}

func signalAlertLifecycleRecheckReconciled() {
	if lifecycleRecheckScheduler != nil {
		lifecycleRecheckScheduler.SignalReconciled()
	}
}
