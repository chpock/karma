package alertlifecycle

import (
	"context"
	"errors"
	"testing"
)

func TestRecheckInfrastructureVocabularyUsesExactPairs(t *testing.T) {
	valid := []struct {
		stage  string
		reason string
	}{
		{stage: "batch", reason: "writer_unavailable"},
		{stage: "batch", reason: "writer_invariant"},
		{stage: "batch", reason: "rollback_failed"},
		{stage: "batch", reason: "store_closing"},
		{stage: "batch", reason: "store_closed"},
		{stage: "batch", reason: "worker_cancelled"},
		{stage: "batch", reason: "worker_deadline"},
		{stage: "batch", reason: "infrastructure_unclassified"},
		{stage: "batch", reason: "queue_read_terminal"},
		{stage: "failure_persistence", reason: "failure_persistence_sql_error"},
		{stage: "failure_persistence", reason: "failure_persistence_rollback_failed"},
		{stage: "failure_persistence", reason: "failure_persistence_writer_invariant"},
		{stage: "failure_persistence", reason: "failure_persistence_writer_unavailable"},
		{stage: "failure_persistence", reason: "failure_persistence_store_closing"},
		{stage: "failure_persistence", reason: "failure_persistence_store_closed"},
		{stage: "failure_persistence", reason: "failure_persistence_cancelled"},
		{stage: "failure_persistence", reason: "failure_persistence_deadline"},
	}
	for _, pair := range valid {
		if !ValidRecheckInfrastructurePair(pair.stage, pair.reason) {
			t.Fatalf("valid pair rejected: %s/%s", pair.stage, pair.reason)
		}
	}
	for _, pair := range [][2]string{{"batch", "custom_cancelled"}, {"failure_persistence", "failure_persistence_custom"}, {"failure_persistence", "failure_persistence_commit_unknown"}, {"failure_persistence", "failure_persistence_not_committed"}, {"batch", "commit_not_committed"}} {
		if ValidRecheckInfrastructurePair(pair[0], pair[1]) {
			t.Fatalf("invalid pair accepted: %s/%s", pair[0], pair[1])
		}
	}
}

func TestRecheckInfrastructureInvalidPairIsRejectedBeforeLogging(t *testing.T) {
	if ValidRecheckInfrastructurePair("batch", "custom_cancelled") {
		t.Fatal("invalid infrastructure pair was accepted")
	}
}

func TestRecheckFailurePersistenceReasonRoutesOnlyTerminalBookkeepingCauses(t *testing.T) {
	cases := []struct {
		name   string
		err    error
		reason string
	}{
		{name: "sql error", err: errors.New("storage failure"), reason: "failure_persistence_sql_error"},
		{name: "rollback precedence", err: errors.Join(errWriterRollbackFailed, ErrWriterOperationInvariant), reason: "failure_persistence_rollback_failed"},
		{name: "invariant", err: ErrWriterOperationInvariant, reason: "failure_persistence_writer_invariant"},
		{name: "writer unavailable", err: ErrWriterUnavailable, reason: "failure_persistence_writer_unavailable"},
		{name: "closing", err: ErrStoreClosing, reason: "failure_persistence_store_closing"},
		{name: "closed", err: ErrStoreClosed, reason: "failure_persistence_store_closed"},
		{name: "cancelled", err: context.Canceled, reason: "failure_persistence_cancelled"},
		{name: "deadline", err: context.DeadlineExceeded, reason: "failure_persistence_deadline"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := RecheckFailurePersistenceReason(tc.err); got != tc.reason {
				t.Fatalf("reason=%q want=%q", got, tc.reason)
			}
			if !ValidRecheckInfrastructurePair("failure_persistence", tc.reason) {
				t.Fatalf("classified reason is not registered: %s", tc.reason)
			}
		})
	}
	for _, reason := range []string{"failure_persistence_commit_unknown", "failure_persistence_not_committed"} {
		if ValidRecheckInfrastructurePair("failure_persistence", reason) {
			t.Fatalf("forbidden commit outcome reason is registered: %q", reason)
		}
	}
	for _, err := range []error{NewRecheckWriterCommitUnknownError("recheck_failure", errors.New("commit unknown")), errors.New("commit not committed")} {
		if reason := RecheckFailurePersistenceReason(err); reason == "failure_persistence_commit_unknown" || reason == "failure_persistence_not_committed" {
			t.Fatalf("commit outcome leaked into terminal reason: %q", reason)
		}
	}
}
