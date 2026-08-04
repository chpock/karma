package alertlifecycle

import "errors"

var (
	ErrAlertNotFound             = errors.New("alert not found")
	ErrResolveStateConflict      = errors.New("resolve is only allowed from inactive state")
	ErrAlertAlreadyResolved      = errors.New("alert already resolved")
	ErrInvalidAlertID            = errors.New("alertID cannot be empty")
	ErrStoreNotInitialized       = errors.New("store is not initialized")
	ErrRelevantUpstreamsCorrupt  = errors.New("relevant upstream list cannot be empty")
)
