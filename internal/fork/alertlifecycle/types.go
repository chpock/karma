package alertlifecycle

import "time"

type State string

const (
	StateActive      State = "active"
	StateSuppressed  State = "suppressed"
	StateUnprocessed State = "unprocessed"
	StateInactive    State = "inactive"
	StateResolved    State = "resolved"
)

type ChangeSource string

const (
	ChangeSourcePoller  ChangeSource = "poller"
	ChangeSourceManual  ChangeSource = "manual"
	ChangeSourceSilence ChangeSource = "silence"
)

type SourceRef struct {
	Name    string `json:"name"`
	Cluster string `json:"cluster"`
}

type SilenceMatcherSnapshot struct {
	Name    string `json:"name"`
	Value   string `json:"value"`
	IsRegex bool   `json:"isRegex"`
	IsEqual bool   `json:"isEqual"`
}

type SourceSilenceSnapshot struct {
	SilenceID       string
	Status          string
	StartsAt        time.Time
	EndsAt          time.Time
	CreatedBy       string
	Comment         string
	Matchers        []SilenceMatcherSnapshot
	UpdatedAt       *time.Time
	AnnotationsJSON string
	TicketID        string
	TicketURL       string
	RawPayloadJSON  string
}

type SourceSilenceRef struct {
	SourceName string
	ClusterID  string
	SilenceIDs []string
}

type SourceSilenceState struct {
	SourceName string
	ClusterID  string
	SyncStatus string
	LastError  string
	Silences   []SourceSilenceSnapshot
}

type AlertCurrent struct {
	AlertID         string
	State           State
	RawState        State
	IsLive          bool
	IsSuppressed    bool
	IsResolved      bool
	Receiver        string
	GroupID         string
	GroupLabelsJSON string
	LabelsJSON      string
	MatchLabelsJSON *string
	AnnotationsJSON string
	StartsAt        time.Time
	LastSeenAt      *time.Time
	// Legacy v4 field; retained during transition to v5 relevant source objects.
	RelevantUpstreams     string
	RelevantSources       string
	SuppressionObservedAt *time.Time
	ResolvedAt            *time.Time
	ResolvedBy            *string
	ResolvedComment       *string
	UpdatedAt             time.Time
}

type AlertStateTransition struct {
	ID              int64
	AlertID         string
	OldState        *State
	NewState        State
	OldRawState     *State
	NewRawState     State
	OldIsLive       *bool
	NewIsLive       bool
	OldIsSuppressed *bool
	NewIsSuppressed bool
	OldIsResolved   *bool
	NewIsResolved   bool
	ChangedAt       time.Time
	ChangeSource    ChangeSource
	ChangedBy       *string
	ReasonCode      *string
	ReasonText      *string
}

type LiveAlertSnapshot struct {
	AlertID         string
	State           State
	Receiver        string
	GroupID         string
	GroupLabelsJSON string
	LabelsJSON      string
	MatchLabelsJSON *string
	AnnotationsJSON string
	StartsAt        time.Time
	LastSeenAt      time.Time
	// Legacy v4 field; retained during transition to v5 relevant source objects.
	RelevantUpstreams []string
	RelevantSources   []SourceRef
	SilencedBySources []SourceSilenceRef
}

type PollCycleInput struct {
	LiveAlerts          []LiveAlertSnapshot
	SuccessfulUpstreams map[string]bool
	SilenceSources      []SourceSilenceState
	Timestamp           time.Time
}

type PollCycleResult struct {
	Inserted          int
	Updated           int
	SetInactive       int
	Reactivated       int
	SkippedInactive   int
	Transitions       int
	RecheckJobsQueued int
}

type ResolveResult struct {
	AlertID   string
	OldState  State
	NewState  State
	Applied   bool
	ChangedAt time.Time
	ChangedBy *string
}

type HousekeepingResult struct {
	ExpiredDeleted        int
	LimitDeleted          int
	VacuumPagesFreed      int
	CurrentRowsAfter      int
	HistoryRowsAfter      int
	RunAt                 time.Time
	LastIncrementalVacuum time.Time
}

type Stats struct {
	CurrentRows                int
	HistoryRows                int
	StateCounts                map[string]int
	SuppressedBreakdownCurrent int
	SuppressedBreakdownPast    int
	GroupingFingerprint        string
	RegroupTargetFingerprint   string
	RegroupStatus              string
	RegroupError               string
	RegroupProcessed           int
	RegroupTotal               int
	RegroupStartedAt           *time.Time
	RegroupFinishedAt          *time.Time
	FileSizeBytes              int64
	PageCount                  int
	PageSizeBytes              int
	FreelistCount              int
	LastHousekeepingAt         *time.Time
	LastIncrementalVacuumAt    *time.Time
}

type RegroupResult struct {
	Processed int
	Updated   int
	Total     int
}
