package main

import (
	"errors"
	"reflect"
	"testing"
)

func TestPIDLeaseSharedReleasePrimitivesUsesPhaseSpecificOwnershipClassification(t *testing.T) {
	missingErr := errors.New("missing-path-shaped identity error")

	t.Run("retained identity capture preserves lease", func(t *testing.T) {
		var calls []string
		file := &problem6MatrixFile{close: func() error { calls = append(calls, "owned-close"); return nil }}
		lease := &pidFileLease{
			configuredPath: "configured.pid",
			resolvedPath:   "resolved.pid",
			owned:          true,
			file:           file,
			ops:            &pidFileOps{},
		}
		initial := snapshotProblem6Lease(lease)
		primitives := pidLeaseReleasePrimitives{
			captureIdentity: func(*pidFileLease) (pidFileIdentity, error) {
				calls = append(calls, "owned-identity")
				return pidFileIdentity{}, missingErr
			},
			captureIdentityOwnershipLost: func(error) bool { return false },
		}

		err := pidLeaseReleaseWithPrimitives(lease, primitives)
		if !errors.Is(err, missingErr) || !errors.Is(err, ErrPIDLeaseIdentityUnavailable) {
			t.Fatalf("retained identity error: %v", err)
		}
		assertProblem6LeaseSnapshotEqual(t, snapshotProblem6Lease(lease), initial)
		if want := []string{"owned-identity"}; !reflect.DeepEqual(calls, want) {
			t.Fatalf("retained identity order: got=%v want=%v", calls, want)
		}
	})

	t.Run("current path open clears lease", func(t *testing.T) {
		ownedCloseErr := errors.New("owned close failed")
		var calls []string
		file := &problem6MatrixFile{close: func() error { calls = append(calls, "owned-close"); return ownedCloseErr }}
		lease := &pidFileLease{
			configuredPath: "configured.pid",
			resolvedPath:   "resolved.pid",
			owned:          true,
			file:           file,
			identity:       problem6NonZeroIdentity(),
			identityValid:  true,
			ops:            &pidFileOps{},
		}
		expected := problem6LeaseSnapshot{ops: lease.ops}
		primitives := pidLeaseReleasePrimitives{
			captureIdentityOwnershipLost: func(error) bool { return false },
			openCurrent: func(*pidFileLease) pidLeaseCurrentResult {
				calls = append(calls, "current-open")
				return pidLeaseCurrentResult{err: missingErr, ownershipLost: true}
			},
		}

		err := pidLeaseReleaseWithPrimitives(lease, primitives)
		if !errors.Is(err, ErrPIDLeaseOwnershipLost) || !errors.Is(err, ownedCloseErr) {
			t.Fatalf("current open ownership error: %v", err)
		}
		assertProblem6LeaseSnapshotEqual(t, snapshotProblem6Lease(lease), expected)
		if want := []string{"current-open", "owned-close"}; !reflect.DeepEqual(calls, want) {
			t.Fatalf("current open ownership order: got=%v want=%v", calls, want)
		}
	})

	t.Run("current identity read preserves lease", func(t *testing.T) {
		currentCloseErr := errors.New("current close failed")
		var calls []string
		file := &problem6MatrixFile{close: func() error { calls = append(calls, "owned-close"); return nil }}
		lease := &pidFileLease{
			configuredPath: "configured.pid",
			resolvedPath:   "resolved.pid",
			owned:          true,
			file:           file,
			identity:       problem6NonZeroIdentity(),
			identityValid:  true,
			ops:            &pidFileOps{},
		}
		initial := snapshotProblem6Lease(lease)
		primitives := pidLeaseReleasePrimitives{
			captureIdentityOwnershipLost: func(error) bool { return false },
			openCurrent: func(*pidFileLease) pidLeaseCurrentResult {
				calls = append(calls, "current-open")
				return pidLeaseCurrentResult{
					handle: &pidLeaseCurrentHandle{close: func() error { calls = append(calls, "current-close"); return currentCloseErr }},
					err:    missingErr,
				}
			},
		}

		err := pidLeaseReleaseWithPrimitives(lease, primitives)
		if !errors.Is(err, missingErr) || !errors.Is(err, currentCloseErr) || errors.Is(err, ErrPIDLeaseOwnershipLost) {
			t.Fatalf("current identity error: %v", err)
		}
		assertProblem6LeaseSnapshotEqual(t, snapshotProblem6Lease(lease), initial)
		if want := []string{"current-open", "current-close"}; !reflect.DeepEqual(calls, want) {
			t.Fatalf("current identity order: got=%v want=%v", calls, want)
		}
	})
}

func TestPIDLeaseSharedReleasePrimitivesOpenCurrentOwnershipLossClosesCurrentAndOwned(t *testing.T) {
	missingErr := errors.New("reparse-shaped identity error")
	currentCloseErr := errors.New("current close failed")
	ownedCloseErr := errors.New("owned close failed")
	var calls []string
	file := &problem6MatrixFile{close: func() error { calls = append(calls, "owned-close"); return ownedCloseErr }}
	lease := &pidFileLease{
		configuredPath: "configured.pid",
		resolvedPath:   "resolved.pid",
		owned:          true,
		file:           file,
		identity:       problem6NonZeroIdentity(),
		identityValid:  true,
		ops:            &pidFileOps{},
	}
	expected := problem6LeaseSnapshot{ops: lease.ops}
	primitives := pidLeaseReleasePrimitives{
		captureIdentityOwnershipLost: func(error) bool { return false },
		openCurrent: func(*pidFileLease) pidLeaseCurrentResult {
			calls = append(calls, "current-open")
			return pidLeaseCurrentResult{
				handle:        &pidLeaseCurrentHandle{close: func() error { calls = append(calls, "current-close"); return currentCloseErr }},
				err:           missingErr,
				ownershipLost: true,
			}
		},
	}

	err := pidLeaseReleaseWithPrimitives(lease, primitives)
	if !errors.Is(err, ErrPIDLeaseOwnershipLost) || !errors.Is(err, currentCloseErr) || !errors.Is(err, ownedCloseErr) {
		t.Fatalf("current identity ownership errors: %v", err)
	}
	assertProblem6LeaseSnapshotEqual(t, snapshotProblem6Lease(lease), expected)
	if want := []string{"current-open", "current-close", "owned-close"}; !reflect.DeepEqual(calls, want) {
		t.Fatalf("current identity ownership order: got=%v want=%v", calls, want)
	}
}
