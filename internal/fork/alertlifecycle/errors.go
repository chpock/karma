package alertlifecycle

import "errors"

var (
	ErrAlertNotFound                  = errors.New("alert not found")
	ErrResolveStateConflict           = errors.New("resolve is only allowed for non-live alerts")
	ErrInvalidAlertID                 = errors.New("alertID cannot be empty")
	ErrStoreNotInitialized            = errors.New("store is not initialized")
	ErrRelevantUpstreamsCorrupt       = errors.New("relevant upstream list cannot be empty")
	ErrLegacyPreviewTablesNotMigrated = errors.New("legacy preview tables contain rows and require explicit migration")
)
