package main

import (
	"errors"
	"os"
	"reflect"
	"testing"
)

type problem6MatrixFile struct {
	close func() error
}

func (f *problem6MatrixFile) Write(data []byte) (int, error) { return len(data), nil }

func (f *problem6MatrixFile) Sync() error { return nil }

func (f *problem6MatrixFile) Stat() (os.FileInfo, error) { return nil, errors.New("unused stat") }

func (f *problem6MatrixFile) Close() error {
	if f.close == nil {
		return nil
	}
	return f.close()
}

type problem6LeaseSnapshot struct {
	configuredPath string
	resolvedPath   string
	owned          bool
	file           pidLeaseFile
	identity       pidFileIdentity
	identityValid  bool
	ops            *pidFileOps
}

func snapshotProblem6Lease(lease *pidFileLease) problem6LeaseSnapshot {
	return problem6LeaseSnapshot{
		configuredPath: lease.configuredPath,
		resolvedPath:   lease.resolvedPath,
		owned:          lease.owned,
		file:           lease.file,
		identity:       lease.identity,
		identityValid:  lease.identityValid,
		ops:            lease.ops,
	}
}

func assertProblem6LeaseSnapshotEqual(t *testing.T, got, want problem6LeaseSnapshot) {
	t.Helper()
	if got.configuredPath != want.configuredPath || got.resolvedPath != want.resolvedPath || got.owned != want.owned || got.file != want.file || got.identityValid != want.identityValid || got.ops != want.ops || !reflect.DeepEqual(got.identity, want.identity) {
		t.Fatalf("lease state changed: got=%+v want=%+v", got, want)
	}
}

func TestPIDLeaseSharedReleasePrimitivesRetryIdentityAndPreserveState(t *testing.T) {
	var calls []string
	identityErr := errors.New("temporary identity failure")
	file := &problem6MatrixFile{}
	lease := &pidFileLease{
		configuredPath: "configured.pid",
		resolvedPath:   "resolved.pid",
		owned:          true,
		file:           file,
		identity:       problem6NonZeroIdentity(),
		ops:            &pidFileOps{},
	}
	initial := snapshotProblem6Lease(lease)
	expected := problem6LeaseSnapshot{ops: lease.ops}
	captureCalls := 0
	primitives := pidLeaseReleasePrimitives{
		captureIdentity: func(*pidFileLease) (pidFileIdentity, error) {
			calls = append(calls, "owned-identity")
			captureCalls++
			if captureCalls == 1 {
				return pidFileIdentity{}, identityErr
			}
			return problem6NonZeroIdentity(), nil
		},
		captureIdentityOwnershipLost: func(error) bool { return false },
		openCurrent: func(*pidFileLease) pidLeaseCurrentResult {
			calls = append(calls, "current-open")
			return pidLeaseCurrentResult{handle: &pidLeaseCurrentHandle{
				identity: pidFileIdentity{},
				close: func() error {
					calls = append(calls, "current-close")
					return nil
				},
				delete: func() error {
					calls = append(calls, "current-delete")
					return nil
				},
			}}
		},
		sameIdentity: func(pidFileIdentity, pidFileIdentity) bool { return true },
	}
	file.close = func() error {
		calls = append(calls, "owned-close")
		return nil
	}

	if err := pidLeaseReleaseWithPrimitives(lease, primitives); !errors.Is(err, identityErr) || !errors.Is(err, ErrPIDLeaseIdentityUnavailable) {
		t.Fatalf("first release error: %v", err)
	}
	assertProblem6LeaseSnapshotEqual(t, snapshotProblem6Lease(lease), initial)

	if err := pidLeaseReleaseWithPrimitives(lease, primitives); err != nil {
		t.Fatalf("retry release error: %v", err)
	}
	if want := []string{"owned-identity", "owned-identity", "current-open", "current-delete", "current-close", "owned-close"}; !reflect.DeepEqual(calls, want) {
		t.Fatalf("retry primitive order: got=%v want=%v", calls, want)
	}
	assertProblem6LeaseSnapshotEqual(t, snapshotProblem6Lease(lease), expected)
}

func TestPIDLeaseSharedReleasePrimitivesCurrentInspectionFailureJoinsClose(t *testing.T) {
	inspectionErr := errors.New("current identity read failed")
	currentCloseErr := errors.New("current close failed")
	var calls []string
	file := &problem6MatrixFile{close: func() error { calls = append(calls, "owned-close"); return nil }}
	lease := &pidFileLease{configuredPath: "configured.pid", resolvedPath: "resolved.pid", owned: true, file: file, identity: problem6NonZeroIdentity(), identityValid: true, ops: &pidFileOps{}}
	initial := snapshotProblem6Lease(lease)
	primitives := pidLeaseReleasePrimitives{
		captureIdentity: func(*pidFileLease) (pidFileIdentity, error) {
			calls = append(calls, "owned-identity")
			return pidFileIdentity{}, nil
		},
		captureIdentityOwnershipLost: func(error) bool { return false },
		openCurrent: func(*pidFileLease) pidLeaseCurrentResult {
			calls = append(calls, "current-open")
			return pidLeaseCurrentResult{handle: &pidLeaseCurrentHandle{close: func() error { calls = append(calls, "current-close"); return currentCloseErr }}, err: inspectionErr}
		},
		sameIdentity: func(pidFileIdentity, pidFileIdentity) bool { return true },
	}

	err := pidLeaseReleaseWithPrimitives(lease, primitives)
	if !errors.Is(err, inspectionErr) || !errors.Is(err, currentCloseErr) {
		t.Fatalf("inspection failure errors: %v", err)
	}
	assertProblem6LeaseSnapshotEqual(t, snapshotProblem6Lease(lease), initial)
	if want := []string{"current-open", "current-close"}; !reflect.DeepEqual(calls, want) {
		t.Fatalf("inspection failure order: got=%v want=%v", calls, want)
	}
}

func TestPIDLeaseSharedReleasePrimitivesOwnershipLossJoinsBothCloseErrors(t *testing.T) {
	ownedCloseErr := errors.New("owned close failed")
	currentCloseErr := errors.New("current close failed")
	var calls []string
	file := &problem6MatrixFile{close: func() error { calls = append(calls, "owned-close"); return ownedCloseErr }}
	lease := &pidFileLease{configuredPath: "configured.pid", resolvedPath: "resolved.pid", owned: true, file: file, identity: problem6NonZeroIdentity(), identityValid: true, ops: &pidFileOps{}}
	expected := problem6LeaseSnapshot{ops: lease.ops}
	primitives := pidLeaseReleasePrimitives{
		captureIdentityOwnershipLost: func(error) bool { return false },
		openCurrent: func(*pidFileLease) pidLeaseCurrentResult {
			calls = append(calls, "current-open")
			return pidLeaseCurrentResult{handle: &pidLeaseCurrentHandle{
				identity: pidFileIdentity{},
				close:    func() error { calls = append(calls, "current-close"); return currentCloseErr },
			}}
		},
		sameIdentity: func(pidFileIdentity, pidFileIdentity) bool { return false },
	}

	err := pidLeaseReleaseWithPrimitives(lease, primitives)
	if !errors.Is(err, ErrPIDLeaseOwnershipLost) || !errors.Is(err, currentCloseErr) || !errors.Is(err, ownedCloseErr) {
		t.Fatalf("ownership-loss errors: %v", err)
	}
	assertProblem6LeaseSnapshotEqual(t, snapshotProblem6Lease(lease), expected)
	if want := []string{"current-open", "current-close", "owned-close"}; !reflect.DeepEqual(calls, want) {
		t.Fatalf("ownership-loss order: got=%v want=%v", calls, want)
	}
}

func TestPIDLeaseSharedReleasePrimitivesDeleteFailureRetainsLease(t *testing.T) {
	deleteErr := errors.New("delete failed")
	currentCloseErr := errors.New("current close failed")
	var calls []string
	file := &problem6MatrixFile{}
	file.close = func() error { calls = append(calls, "owned-close"); return nil }
	lease := &pidFileLease{configuredPath: "configured.pid", resolvedPath: "resolved.pid", owned: true, file: file, identity: problem6NonZeroIdentity(), identityValid: true, ops: &pidFileOps{}}
	initial := snapshotProblem6Lease(lease)
	primitives := pidLeaseReleasePrimitives{
		captureIdentityOwnershipLost: func(error) bool { return false },
		openCurrent: func(*pidFileLease) pidLeaseCurrentResult {
			calls = append(calls, "current-open")
			return pidLeaseCurrentResult{handle: &pidLeaseCurrentHandle{
				identity: pidFileIdentity{},
				close:    func() error { calls = append(calls, "current-close"); return currentCloseErr },
				delete:   func() error { calls = append(calls, "current-delete"); return deleteErr },
			}}
		},
		sameIdentity: func(pidFileIdentity, pidFileIdentity) bool { return true },
	}

	err := pidLeaseReleaseWithPrimitives(lease, primitives)
	if !errors.Is(err, deleteErr) || !errors.Is(err, currentCloseErr) {
		t.Fatalf("delete failure errors: %v", err)
	}
	assertProblem6LeaseSnapshotEqual(t, snapshotProblem6Lease(lease), initial)
	if want := []string{"current-open", "current-delete", "current-close"}; !reflect.DeepEqual(calls, want) {
		t.Fatalf("delete failure order: got=%v want=%v", calls, want)
	}
}

func TestPIDLeaseSharedReleasePrimitivesSuccessfulDeleteJoinsBothCloseErrors(t *testing.T) {
	currentCloseErr := errors.New("current close failed")
	ownedCloseErr := errors.New("owned close failed")
	var calls []string
	file := &problem6MatrixFile{close: func() error { calls = append(calls, "owned-close"); return ownedCloseErr }}
	lease := &pidFileLease{configuredPath: "configured.pid", resolvedPath: "resolved.pid", owned: true, file: file, identity: problem6NonZeroIdentity(), identityValid: true, ops: &pidFileOps{}}
	expected := problem6LeaseSnapshot{ops: lease.ops}
	primitives := pidLeaseReleasePrimitives{
		captureIdentityOwnershipLost: func(error) bool { return false },
		openCurrent: func(*pidFileLease) pidLeaseCurrentResult {
			calls = append(calls, "current-open")
			return pidLeaseCurrentResult{handle: &pidLeaseCurrentHandle{
				identity: pidFileIdentity{},
				close:    func() error { calls = append(calls, "current-close"); return currentCloseErr },
				delete:   func() error { calls = append(calls, "current-delete"); return nil },
			}}
		},
		sameIdentity: func(pidFileIdentity, pidFileIdentity) bool { return true },
	}

	err := pidLeaseReleaseWithPrimitives(lease, primitives)
	if !errors.Is(err, currentCloseErr) || !errors.Is(err, ownedCloseErr) {
		t.Fatalf("successful delete errors: %v", err)
	}
	assertProblem6LeaseSnapshotEqual(t, snapshotProblem6Lease(lease), expected)
	if want := []string{"current-open", "current-delete", "current-close", "owned-close"}; !reflect.DeepEqual(calls, want) {
		t.Fatalf("successful delete order: got=%v want=%v", calls, want)
	}
}
