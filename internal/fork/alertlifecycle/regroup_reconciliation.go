package alertlifecycle

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	runtimeLog "github.com/prymitive/karma/internal/log"
)

const regroupReconciliationInterval = time.Second

var regroupReconciliationTimer = func(duration time.Duration) (<-chan time.Time, func()) {
	timer := time.NewTimer(duration)
	return timer.C, func() { timer.Stop() }
}

var (
	ErrRegroupFailurePersisted       = errors.New("regroup failure metadata was persisted")
	ErrRegroupExpectedShutdown       = errors.New("regroup stopped for expected shutdown")
	ErrRegroupInfrastructureContract = errors.New("regroup infrastructure contract violation")
	errWriterRollbackFailed          = errors.New("lifecycle writer rollback failed")
)

type RegroupInfrastructureStage string

const (
	RegroupStageBegin              RegroupInfrastructureStage = "begin"
	RegroupStageBatch              RegroupInfrastructureStage = "batch"
	RegroupStageFailurePersistence RegroupInfrastructureStage = "failure_persistence"
)

type RegroupInfrastructureReason string

const (
	RegroupReasonCommitOutcomeUnknown                RegroupInfrastructureReason = "commit_outcome_unknown"
	RegroupReasonCommitNotCommitted                  RegroupInfrastructureReason = "commit_not_committed"
	RegroupReasonRollbackFailed                      RegroupInfrastructureReason = "rollback_failed"
	RegroupReasonWriterUnavailable                   RegroupInfrastructureReason = "writer_unavailable"
	RegroupReasonWriterInvariant                     RegroupInfrastructureReason = "writer_invariant"
	RegroupReasonStoreClosing                        RegroupInfrastructureReason = "store_closing"
	RegroupReasonStoreClosed                         RegroupInfrastructureReason = "store_closed"
	RegroupReasonCancelled                           RegroupInfrastructureReason = "cancelled"
	RegroupReasonDeadline                            RegroupInfrastructureReason = "deadline"
	RegroupReasonUnclassifiedError                   RegroupInfrastructureReason = "unclassified_error"
	RegroupReasonFinalizerCancelled                  RegroupInfrastructureReason = "finalizer_cancelled"
	RegroupReasonMissingCommittedResult              RegroupInfrastructureReason = "missing_committed_result"
	RegroupReasonFailurePersistenceCommitUnknown     RegroupInfrastructureReason = "failure_persistence_commit_outcome_unknown"
	RegroupReasonFailurePersistenceNotCommitted      RegroupInfrastructureReason = "failure_persistence_commit_not_committed"
	RegroupReasonFailurePersistenceRollbackFailed    RegroupInfrastructureReason = "failure_persistence_rollback_failed"
	RegroupReasonFailurePersistenceWriterUnavailable RegroupInfrastructureReason = "failure_persistence_writer_unavailable"
	RegroupReasonFailurePersistenceWriterInvariant   RegroupInfrastructureReason = "failure_persistence_writer_invariant"
	RegroupReasonFailurePersistenceStoreClosing      RegroupInfrastructureReason = "failure_persistence_store_closing"
	RegroupReasonFailurePersistenceStoreClosed       RegroupInfrastructureReason = "failure_persistence_store_closed"
	RegroupReasonFailurePersistenceCancelled         RegroupInfrastructureReason = "failure_persistence_cancelled"
	RegroupReasonFailurePersistenceDeadline          RegroupInfrastructureReason = "failure_persistence_deadline"
	RegroupReasonFailurePersistenceSQLError          RegroupInfrastructureReason = "failure_persistence_sql_error"
	RegroupReasonFailurePersistenceUnclassified      RegroupInfrastructureReason = "failure_persistence_unclassified_error"
)

type RegroupInfrastructureError struct {
	Stage            RegroupInfrastructureStage
	ReasonCode       RegroupInfrastructureReason
	Processed        int
	Total            int
	Debug            string
	ExpectedShutdown bool
	Err              error
}

func (e *RegroupInfrastructureError) Error() string {
	if e == nil {
		return "regroup infrastructure error"
	}
	return fmt.Sprintf("regroup infrastructure error at %s (%s): %v", e.Stage, e.ReasonCode, e.Err)
}

func (e *RegroupInfrastructureError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func (e *RegroupInfrastructureError) Valid() bool {
	if e == nil {
		return false
	}
	switch e.Stage {
	case RegroupStageBegin, RegroupStageBatch:
		switch string(e.ReasonCode) {
		case string(RegroupReasonCommitOutcomeUnknown), string(RegroupReasonCommitNotCommitted),
			string(RegroupReasonRollbackFailed), string(RegroupReasonWriterUnavailable),
			string(RegroupReasonWriterInvariant), string(RegroupReasonStoreClosing),
			string(RegroupReasonStoreClosed), string(RegroupReasonCancelled), string(RegroupReasonDeadline),
			string(RegroupReasonUnclassifiedError), string(RegroupReasonFinalizerCancelled),
			string(RegroupReasonMissingCommittedResult):
			return true
		}
	case RegroupStageFailurePersistence:
		switch string(e.ReasonCode) {
		case string(RegroupReasonFailurePersistenceCommitUnknown),
			string(RegroupReasonFailurePersistenceNotCommitted),
			string(RegroupReasonFailurePersistenceRollbackFailed),
			string(RegroupReasonFailurePersistenceWriterUnavailable),
			string(RegroupReasonFailurePersistenceWriterInvariant),
			string(RegroupReasonFailurePersistenceStoreClosing),
			string(RegroupReasonFailurePersistenceStoreClosed),
			string(RegroupReasonFailurePersistenceCancelled),
			string(RegroupReasonFailurePersistenceDeadline),
			string(RegroupReasonFailurePersistenceSQLError),
			string(RegroupReasonFailurePersistenceUnclassified):
			return true
		}
	}
	return false
}

type regroupPhaseValue struct {
	begin *regroupStartResult
	batch *writerTupleResult
}

type regroupPhaseCompletion struct {
	mu            sync.Mutex
	committedOnce sync.Once
	finalizedOnce sync.Once
	committedDone chan struct{}
	finalizedDone chan struct{}
	committed     bool
	handoffErr    error
	value         regroupPhaseValue
	event         func(regroupPhaseValue)
	eventEmitted  bool
}

func newRegroupPhaseCompletion(event func(regroupPhaseValue)) *regroupPhaseCompletion {
	return &regroupPhaseCompletion{
		committedDone: make(chan struct{}),
		finalizedDone: make(chan struct{}),
		event:         event,
	}
}

func (c *regroupPhaseCompletion) setBegin(value regroupStartResult) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.value.begin = &value
	c.mu.Unlock()
}

func (c *regroupPhaseCompletion) setBatch(value writerTupleResult) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.value.batch = &value
	c.mu.Unlock()
}

func (c *regroupPhaseCompletion) completeCommitted() {
	if c == nil {
		return
	}
	c.committedOnce.Do(func() {
		c.mu.Lock()
		c.committed = true
		c.mu.Unlock()
		close(c.committedDone)
	})
}

func (c *regroupPhaseCompletion) failHandoff(err error) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.handoffErr = err
	c.mu.Unlock()
	c.completeCommitted()
	c.finalizedOnce.Do(func() {
		c.mu.Lock()
		c.eventEmitted = true
		c.mu.Unlock()
		close(c.finalizedDone)
	})
}

func (c *regroupPhaseCompletion) isCommitted() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.committed
}

func (c *regroupPhaseCompletion) valueSnapshot() regroupPhaseValue {
	if c == nil {
		return regroupPhaseValue{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.value
}

func (c *regroupPhaseCompletion) finalize() {
	if c == nil {
		return
	}
	c.finalizedOnce.Do(func() {
		c.mu.Lock()
		c.eventEmitted = true
		committed := c.committed
		event := c.event
		value := c.value
		c.mu.Unlock()
		defer close(c.finalizedDone)
		if committed && event != nil {
			event(value)
		}
	})
}

func newRegroupPhaseFinalizer(ctx context.Context, operationKind string, completion *regroupPhaseCompletion) *writerAfterCommitFinalizer {
	return &writerAfterCommitFinalizer{
		ctx:           ctx,
		operationKind: operationKind,
		handoff: func(writerHandoffDisposition) {
			completion.completeCommitted()
		},
		onHandoffError: func(err error) {
			completion.failHandoff(err)
		},
		run: func(bool) {
			completion.finalize()
		},
	}
}

func (c *regroupPhaseCompletion) waitFinalized(ctx context.Context) error {
	if c == nil {
		return nil
	}
	select {
	case <-c.finalizedDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func waitRegroupCommitted(ctx context.Context, completion *regroupPhaseCompletion) (bool, error) {
	if completion.isCommitted() {
		return true, nil
	}
	timer, stopTimer := regroupReconciliationTimer(regroupReconciliationInterval)
	defer stopTimer()
	select {
	case <-completion.committedDone:
		return true, nil
	case <-timer:
		return false, nil
	case <-ctx.Done():
		if completion.isCommitted() {
			return true, nil
		}
		return false, ctx.Err()
	}
}

func regroupInfrastructureReason(ctx context.Context, err error) RegroupInfrastructureReason {
	if err == nil {
		return ""
	}
	var acquisitionErr *writerAcquisitionError
	if errors.As(err, &acquisitionErr) {
		return RegroupReasonWriterUnavailable
	}
	if errors.Is(err, ErrWriterOperationInvariant) {
		return RegroupReasonWriterInvariant
	}
	var outcome *writerCommitOutcomeError
	if errors.As(err, &outcome) {
		switch outcome.outcome {
		case writerCommitOutcomeUnknown:
			return RegroupReasonCommitOutcomeUnknown
		case writerCommitOutcomeNotCommitted:
			return RegroupReasonCommitNotCommitted
		case writerCommitOutcomeCommitted:
			return ""
		}
	}
	if errors.Is(err, errWriterRollbackFailed) {
		return RegroupReasonRollbackFailed
	}
	if errors.Is(err, ErrWriterUnavailable) {
		return RegroupReasonWriterUnavailable
	}
	if errors.Is(err, ErrStoreClosing) {
		return RegroupReasonStoreClosing
	}
	if errors.Is(err, ErrStoreClosed) {
		return RegroupReasonStoreClosed
	}
	if ctx != nil && errors.Is(err, context.Canceled) && (errors.Is(ctx.Err(), context.Canceled) || errors.Is(context.Cause(ctx), context.Canceled)) {
		return RegroupReasonCancelled
	}
	if ctx != nil && errors.Is(err, context.DeadlineExceeded) && (errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(context.Cause(ctx), context.DeadlineExceeded)) {
		return RegroupReasonDeadline
	}
	return ""
}

func newRegroupInfrastructureError(ctx context.Context, stage RegroupInfrastructureStage, reason RegroupInfrastructureReason, result RegroupResult, err error) error {
	infra := &RegroupInfrastructureError{
		Stage:            stage,
		ReasonCode:       reason,
		Processed:        result.Processed,
		Total:            result.Total,
		Debug:            errorString(err),
		ExpectedShutdown: ctx != nil && errors.Is(context.Cause(ctx), ErrRegroupExpectedShutdown),
		Err:              err,
	}
	if !infra.Valid() {
		return errors.Join(ErrWriterOperationInvariant, ErrRegroupInfrastructureContract, infra)
	}
	return infra
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func (s *Store) runRegroupToConfigWithReconciliation(ctx context.Context, cfg GroupingConfig, batchSize int) (RegroupResult, error) {
	if s == nil || s.db == nil {
		return RegroupResult{}, ErrStoreNotInitialized
	}
	if batchSize < 1 {
		batchSize = 1000
	}
	targetFingerprint := BuildGroupingFingerprint(cfg)

	beginCompletion := newRegroupPhaseCompletion(func(value regroupPhaseValue) {
		if value.begin == nil || !value.begin.started {
			return
		}
		if value.begin.resumed {
			runtimeLog.InfoContext(ctx, "lifecycle_regroup", "regroup_resumed", "Alert lifecycle regroup resumed", slog.String("target_fingerprint", targetFingerprint), slog.Int("total", value.begin.total))
			return
		}
		runtimeLog.InfoContext(ctx, "lifecycle_regroup", "regroup_started", "Alert lifecycle regroup started", slog.String("target_fingerprint", targetFingerprint), slog.Int("total", value.begin.total), slog.Int("batch_size", batchSize))
	})
	startValue, err := s.runRegroupBeginWithReconciliation(ctx, targetFingerprint, beginCompletion)
	if err != nil {
		return s.persistRegroupFailureOrReturn(ctx, targetFingerprint, RegroupResult{Processed: -1, Total: -1}, RegroupStageBegin, err)
	}
	if !startValue.started {
		return RegroupResult{Total: startValue.total}, nil
	}

	compiled, err := compileGroupingConfig(cfg)
	if err != nil {
		return s.persistRegroupFailureOrReturn(ctx, targetFingerprint, RegroupResult{Total: startValue.total}, RegroupStageBegin, fmt.Errorf("invalid grouping config: %w", err))
	}
	result := RegroupResult{Total: startValue.total}
	for {
		if ctx != nil {
			select {
			case <-ctx.Done():
				cancelErr := ctx.Err()
				if errors.Is(context.Cause(ctx), ErrRegroupExpectedShutdown) {
					return result, newRegroupInfrastructureError(ctx, RegroupStageBatch, RegroupReasonCancelled, result, errors.Join(cancelErr, context.Cause(ctx)))
				}
				if reason := regroupInfrastructureReason(ctx, cancelErr); reason != "" {
					return result, newRegroupInfrastructureError(ctx, RegroupStageBatch, reason, result, cancelErr)
				}
				return result, cancelErr
			default:
			}
		}
		batchStartedAt := time.Now()
		completion := newRegroupPhaseCompletion(func(value regroupPhaseValue) {
			if value.batch == nil {
				return
			}
			runtimeLog.DebugContext(ctx, "lifecycle_regroup", "regroup_batch_completed", "Alert lifecycle regroup batch completed", slog.Int("processed", value.batch.processed), slog.Int("updated", value.batch.updated), slog.Int("processed_total", result.Processed+value.batch.processed), slog.Int("total", value.batch.total), slog.Duration("duration", time.Since(batchStartedAt)))
		})
		batch, batchErr := s.runRegroupBatchWithReconciliation(ctx, targetFingerprint, compiled, batchSize, completion)
		if batchErr != nil {
			return s.persistRegroupFailureOrReturn(ctx, targetFingerprint, result, RegroupStageBatch, batchErr)
		}
		result.Processed += batch.processed
		result.Updated += batch.updated
		result.Total = batch.total
		if batch.done {
			return result, nil
		}
	}
}

func (s *Store) runRegroupBeginWithReconciliation(ctx context.Context, targetFingerprint string, completion *regroupPhaseCompletion) (regroupStartResult, error) {
	for {
		if completion.isCommitted() {
			if waitErr := completion.waitFinalized(ctx); waitErr != nil {
				return regroupStartResult{}, newRegroupInfrastructureError(ctx, RegroupStageBegin, RegroupReasonFinalizerCancelled, RegroupResult{}, waitErr)
			}
			value := completion.valueSnapshot()
			if value.begin == nil {
				return regroupStartResult{}, newRegroupInfrastructureError(ctx, RegroupStageBegin, RegroupReasonMissingCommittedResult, RegroupResult{}, ErrWriterOperationInvariant)
			}
			return *value.begin, nil
		}
		finalizer := newRegroupPhaseFinalizer(ctx, writerOperationRegroupBegin, completion)
		value, err := s.beginOrResumeRegroupPhase(ctx, targetFingerprint, completion, finalizer)
		if err == nil {
			_ = completion.waitFinalized(ctx)
			return value, nil
		}
		if errors.Is(err, ErrWriterCommitReconciled) {
			if completion.isCommitted() {
				if waitErr := completion.waitFinalized(ctx); waitErr != nil {
					return regroupStartResult{}, newRegroupInfrastructureError(ctx, RegroupStageBegin, RegroupReasonFinalizerCancelled, RegroupResult{}, waitErr)
				}
				value := completion.valueSnapshot()
				if value.begin == nil {
					return regroupStartResult{}, newRegroupInfrastructureError(ctx, RegroupStageBegin, RegroupReasonMissingCommittedResult, RegroupResult{}, ErrWriterOperationInvariant)
				}
				return *value.begin, nil
			}
			continue
		}
		if reason := regroupInfrastructureReason(ctx, err); reason != "" {
			if reason == RegroupReasonCommitOutcomeUnknown || reason == RegroupReasonCommitNotCommitted {
				committed, waitErr := waitRegroupCommitted(ctx, completion)
				if waitErr != nil {
					return regroupStartResult{}, newRegroupInfrastructureError(ctx, RegroupStageBegin, reason, RegroupResult{}, waitErr)
				}
				if committed {
					if waitErr := completion.waitFinalized(ctx); waitErr != nil {
						return regroupStartResult{}, newRegroupInfrastructureError(ctx, RegroupStageBegin, RegroupReasonFinalizerCancelled, RegroupResult{}, waitErr)
					}
					value := completion.valueSnapshot()
					if value.begin == nil {
						return regroupStartResult{}, newRegroupInfrastructureError(ctx, RegroupStageBegin, RegroupReasonMissingCommittedResult, RegroupResult{}, ErrWriterOperationInvariant)
					}
					return *value.begin, nil
				}
				continue
			}
			return regroupStartResult{}, newRegroupInfrastructureError(ctx, RegroupStageBegin, reason, RegroupResult{}, err)
		}
		return regroupStartResult{}, err
	}
}

func (s *Store) runRegroupBatchWithReconciliation(ctx context.Context, targetFingerprint string, cfg compiledGroupingConfig, batchSize int, completion *regroupPhaseCompletion) (writerTupleResult, error) {
	for {
		if completion.isCommitted() {
			if waitErr := completion.waitFinalized(ctx); waitErr != nil {
				return writerTupleResult{}, newRegroupInfrastructureError(ctx, RegroupStageBatch, RegroupReasonFinalizerCancelled, RegroupResult{}, waitErr)
			}
			value := completion.valueSnapshot()
			if value.batch == nil {
				return writerTupleResult{}, newRegroupInfrastructureError(ctx, RegroupStageBatch, RegroupReasonMissingCommittedResult, RegroupResult{}, ErrWriterOperationInvariant)
			}
			return *value.batch, nil
		}
		finalizer := newRegroupPhaseFinalizer(ctx, writerOperationRegroupBatch, completion)
		value, err := s.runRegroupBatchPhase(ctx, targetFingerprint, cfg, batchSize, completion, finalizer)
		if err == nil {
			_ = completion.waitFinalized(ctx)
			return value, nil
		}
		if errors.Is(err, ErrWriterCommitReconciled) {
			if completion.isCommitted() {
				if waitErr := completion.waitFinalized(ctx); waitErr != nil {
					return writerTupleResult{}, newRegroupInfrastructureError(ctx, RegroupStageBatch, RegroupReasonFinalizerCancelled, RegroupResult{}, waitErr)
				}
				value := completion.valueSnapshot()
				if value.batch == nil {
					return writerTupleResult{}, newRegroupInfrastructureError(ctx, RegroupStageBatch, RegroupReasonMissingCommittedResult, RegroupResult{}, ErrWriterOperationInvariant)
				}
				return *value.batch, nil
			}
			continue
		}
		if reason := regroupInfrastructureReason(ctx, err); reason != "" {
			if reason == RegroupReasonCommitOutcomeUnknown || reason == RegroupReasonCommitNotCommitted {
				committed, waitErr := waitRegroupCommitted(ctx, completion)
				if waitErr != nil {
					return writerTupleResult{}, newRegroupInfrastructureError(ctx, RegroupStageBatch, reason, RegroupResult{}, waitErr)
				}
				if committed {
					if waitErr := completion.waitFinalized(ctx); waitErr != nil {
						return writerTupleResult{}, newRegroupInfrastructureError(ctx, RegroupStageBatch, RegroupReasonFinalizerCancelled, RegroupResult{}, waitErr)
					}
					value := completion.valueSnapshot()
					if value.batch == nil {
						return writerTupleResult{}, newRegroupInfrastructureError(ctx, RegroupStageBatch, RegroupReasonMissingCommittedResult, RegroupResult{}, ErrWriterOperationInvariant)
					}
					return *value.batch, nil
				}
				continue
			}
			return writerTupleResult{}, newRegroupInfrastructureError(ctx, RegroupStageBatch, reason, RegroupResult{}, err)
		}
		return writerTupleResult{}, err
	}
}

func (s *Store) persistRegroupFailureOrReturn(ctx context.Context, targetFingerprint string, result RegroupResult, stage RegroupInfrastructureStage, domainErr error) (RegroupResult, error) {
	if reason := regroupInfrastructureReason(ctx, domainErr); reason != "" {
		return result, newRegroupInfrastructureError(ctx, stage, reason, result, domainErr)
	}
	return result, s.persistRegroupFailure(ctx, targetFingerprint, result, domainErr)
}

type regroupFailureSnapshot struct {
	targetFingerprint string
	processed         int
	total             int
	finishedAt        time.Time
	rootError         string
}

func (s *Store) persistRegroupFailure(ctx context.Context, targetFingerprint string, result RegroupResult, domainErr error) error {
	completion := newRecheckFailureCompletion()
	startedAt := time.Now()
	var snapshot regroupFailureSnapshot
	newFinalizer := func() *writerAfterCommitFinalizer {
		return &writerAfterCommitFinalizer{
			ctx:           ctx,
			operationKind: writerOperationRegroupFailure,
			handoff:       func(writerHandoffDisposition) { completion.completeCommitted() },
			onHandoffError: func(err error) {
				_ = err
				completion.completeCommitted()
				completion.completeFinalized()
			},
			run: func(bool) {
				defer completion.completeFinalized()
				if completion.isCommitted() {
					runtimeLog.ErrorContext(ctx, "lifecycle_regroup", "regroup_failed", "Alert lifecycle regroup failed", slog.String("target_fingerprint", snapshot.targetFingerprint), slog.Int("processed", snapshot.processed), slog.Int("total", snapshot.total), slog.String("error", SanitizeRecheckError(errors.New(snapshot.rootError))), slog.Time("finished_at", snapshot.finishedAt), slog.Duration("duration", time.Since(startedAt)))
				}
			},
		}
	}
	finalizer := newFinalizer()

	for {
		_, persistErr := withWriterTxValueAfterCommit(ctx, s, writerOperationRegroupFailure, func(tx *sql.Tx) (struct{}, error) {
			processed, err := readMetadataIntTx(tx, metaRegroupProcessed)
			if err != nil {
				return struct{}{}, err
			}
			total, err := readMetadataIntTx(tx, metaRegroupTotal)
			if err != nil {
				return struct{}{}, err
			}
			finishedAt := s.currentRegroupTime()
			if err := setMetadataTx(tx, metaRegroupStatus, regroupStatusError); err != nil {
				return struct{}{}, err
			}
			if err := setMetadataTx(tx, metaRegroupError, strings.TrimSpace(domainErr.Error())); err != nil {
				return struct{}{}, err
			}
			if err := setMetadataTimeTx(tx, metaRegroupFinishedAt, &finishedAt); err != nil {
				return struct{}{}, err
			}
			snapshot = regroupFailureSnapshot{targetFingerprint: targetFingerprint, processed: processed, total: total, finishedAt: finishedAt, rootError: domainErr.Error()}
			return struct{}{}, nil
		}, finalizer)
		if persistErr == nil {
			return errors.Join(domainErr, ErrRegroupFailurePersisted)
		}
		if errors.Is(persistErr, ErrWriterCommitReconciled) {
			if completion.isCommitted() {
				return errors.Join(domainErr, ErrRegroupFailurePersisted)
			}
			continue
		}
		reason := regroupInfrastructureReason(ctx, persistErr)
		if reason == RegroupReasonCommitOutcomeUnknown {
			committed, waitErr := waitRegroupFailureCommitted(ctx, completion)
			if waitErr != nil {
				return errors.Join(domainErr, newRegroupInfrastructureError(ctx, RegroupStageFailurePersistence, regroupFailurePersistenceReason(ctx, waitErr), result, waitErr))
			}
			if committed {
				if waitErr = completion.waitFinalized(ctx); waitErr != nil {
					return errors.Join(domainErr, newRegroupInfrastructureError(ctx, RegroupStageFailurePersistence, regroupFailurePersistenceReason(ctx, waitErr), result, waitErr))
				}
				return errors.Join(domainErr, ErrRegroupFailurePersisted)
			}
			finalizer = newFinalizer()
			continue
		}
		return errors.Join(domainErr, newRegroupInfrastructureError(ctx, RegroupStageFailurePersistence, regroupFailurePersistenceReason(ctx, persistErr), result, persistErr))
	}
}

func regroupFailurePersistenceReason(ctx context.Context, err error) RegroupInfrastructureReason {
	switch string(regroupInfrastructureReason(ctx, err)) {
	case string(RegroupReasonCommitOutcomeUnknown):
		return RegroupReasonFailurePersistenceCommitUnknown
	case string(RegroupReasonCommitNotCommitted):
		return RegroupReasonFailurePersistenceNotCommitted
	case string(RegroupReasonRollbackFailed):
		return RegroupReasonFailurePersistenceRollbackFailed
	case string(RegroupReasonWriterUnavailable):
		return RegroupReasonFailurePersistenceWriterUnavailable
	case string(RegroupReasonWriterInvariant):
		return RegroupReasonFailurePersistenceWriterInvariant
	case string(RegroupReasonStoreClosing):
		return RegroupReasonFailurePersistenceStoreClosing
	case string(RegroupReasonStoreClosed):
		return RegroupReasonFailurePersistenceStoreClosed
	case string(RegroupReasonCancelled):
		return RegroupReasonFailurePersistenceCancelled
	case string(RegroupReasonDeadline):
		return RegroupReasonFailurePersistenceDeadline
	default:
		return RegroupReasonFailurePersistenceSQLError
	}
}

func waitRegroupFailureCommitted(ctx context.Context, completion *recheckFailureCompletion) (bool, error) {
	if completion.isCommitted() {
		return true, nil
	}
	timer, stopTimer := regroupReconciliationTimer(regroupReconciliationInterval)
	defer stopTimer()
	select {
	case <-completion.committedDone:
		return true, nil
	case <-timer:
		return false, nil
	case <-ctx.Done():
		if completion.isCommitted() {
			return true, nil
		}
		return false, ctx.Err()
	}
}
