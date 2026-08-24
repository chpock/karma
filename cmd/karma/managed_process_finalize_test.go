package main

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestManagedFinalizeStatusPublishesExitAfterCleanup(t *testing.T) {
	var events []string
	ops := managedFinalizeOperations{
		writeFile: func(_, name, _ string) error {
			events = append(events, "write:"+name)
			return nil
		},
		cleanupProcessGroup: func(_ int, _ time.Duration) error {
			events = append(events, "cleanup")
			return nil
		},
	}

	if code := managedFinalizeStatusWithOperations("state", "exit:7", 123, ops); code != 7 {
		t.Fatalf("finalization code: got=%d want=7", code)
	}
	if want := []string{"cleanup", "write:exit"}; !reflect.DeepEqual(events, want) {
		t.Fatalf("finalization order: got=%v want=%v", events, want)
	}
}

func TestManagedFinalizeStatusPersistsCleanupFailureBeforeExit(t *testing.T) {
	cleanupErr := errors.New("cleanup failed")
	var events []string
	var errorContents string
	ops := managedFinalizeOperations{
		writeFile: func(_, name, contents string) error {
			events = append(events, "write:"+name)
			if name == "harness-error" {
				errorContents = contents
			}
			return nil
		},
		cleanupProcessGroup: func(_ int, _ time.Duration) error {
			events = append(events, "cleanup")
			return cleanupErr
		},
	}

	if code := managedFinalizeStatusWithOperations("state", "exit:0", 123, ops); code != managedProtocolCode {
		t.Fatalf("cleanup failure code: got=%d want=%d", code, managedProtocolCode)
	}
	if want := []string{"cleanup", "write:harness-error", "write:exit"}; !reflect.DeepEqual(events, want) {
		t.Fatalf("cleanup failure order: got=%v want=%v", events, want)
	}
	if !strings.Contains(errorContents, cleanupErr.Error()) {
		t.Fatalf("cleanup failure was not persisted: %q", errorContents)
	}
}

func TestManagedFinalizeStatusFailsClosedWhenExitMarkerWriteFails(t *testing.T) {
	markerErr := errors.New("exit marker failed")
	var events []string
	var errorContents string
	ops := managedFinalizeOperations{
		writeFile: func(_, name, contents string) error {
			events = append(events, "write:"+name)
			if name == "exit" {
				return markerErr
			}
			errorContents = contents
			return nil
		},
		cleanupProcessGroup: func(_ int, _ time.Duration) error {
			events = append(events, "cleanup")
			return nil
		},
	}

	if code := managedFinalizeStatusWithOperations("state", "exit:0", 123, ops); code != managedProtocolCode {
		t.Fatalf("marker failure code: got=%d want=%d", code, managedProtocolCode)
	}
	if want := []string{"cleanup", "write:exit", "write:harness-error"}; !reflect.DeepEqual(events, want) {
		t.Fatalf("marker failure order: got=%v want=%v", events, want)
	}
	if !strings.Contains(errorContents, markerErr.Error()) {
		t.Fatalf("marker failure was not persisted: %q", errorContents)
	}
}
