package alertlifecycle

import (
	"context"
	"errors"
	"log/slog"

	runtimeLog "github.com/prymitive/karma/internal/log"
)

func writerSafeErrorCategory(err error) string {
	if err == nil {
		return "writer_error"
	}
	switch {
	case errors.Is(err, context.Canceled):
		return "cancelled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline"
	case errors.Is(err, ErrWriterCommitOutcomeUnknown):
		return "commit_outcome_unknown"
	case errors.Is(err, ErrWriterUnavailable), IsWriterAcquisitionError(err):
		return "writer_unavailable"
	case errors.Is(err, ErrWriterOperationInvariant), errors.Is(err, ErrWriterOperationToken):
		return "writer_invariant"
	default:
		return "writer_error"
	}
}

func writerSafePanicCategory(value any) string {
	if err, ok := value.(error); ok {
		return writerSafeErrorCategory(err)
	}
	return "panic"
}

func logWriterObservabilityFailure(ctx context.Context, operationKind, stage, category string) {
	defer func() {
		_ = recover()
	}()
	runtimeLog.ErrorContext(ctx, "lifecycle_writer", "writer_observability_failed", "Lifecycle writer observability failed", slog.String("writer_operation", operationKind), slog.String("stage", stage), slog.String("safe_error", category))
}
