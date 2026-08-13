package alertlifecycle

import "slices"

// LifecycleDimensions are the complete state axes persisted for one global or
// target alert projection. State is derived and never accepted as independent
// input.
type LifecycleDimensions struct {
	RawState     State
	IsLive       bool
	IsSuppressed bool
	IsResolved   bool
}

func (d LifecycleDimensions) ProjectedState() State {
	if d.IsLive {
		switch d.RawState {
		case StateActive, StateSuppressed:
			return d.RawState
		case StateUnprocessed, StateInactive, StateResolved:
			return StateUnprocessed
		}
	}
	if d.IsResolved {
		return StateResolved
	}
	if d.IsSuppressed {
		return StateSuppressed
	}
	return StateInactive
}

func ObserveLive(previous LifecycleDimensions, rawState State) LifecycleDimensions {
	previous.RawState = rawState
	previous.IsLive = true
	previous.IsSuppressed = rawState == StateSuppressed
	previous.IsResolved = false
	return previous
}

func ObserveAbsent(previous LifecycleDimensions) LifecycleDimensions {
	previous.IsLive = false
	return previous
}

func ResolveDimensions(previous LifecycleDimensions) (LifecycleDimensions, bool) {
	if previous.IsLive || previous.IsResolved {
		return previous, false
	}
	previous.IsResolved = true
	return previous, true
}

func PromoteSuppressed(previous LifecycleDimensions) (LifecycleDimensions, bool) {
	if previous.IsLive || previous.IsResolved || previous.IsSuppressed || previous.ProjectedState() != StateInactive {
		return previous, false
	}
	previous.IsSuppressed = true
	return previous, true
}

// TargetTopologyObservation contains only topology/coverage facts. An empty
// configured set is authoritative only when ManifestKnown is true.
type TargetTopologyObservation struct {
	PreviousMembers []string
	ObservedMembers []string
	Configured      map[string]struct{}
	ManifestKnown   bool
	Successful      map[string]bool
}

type TargetTopologyPlan struct {
	Members  []string
	Retire   bool
	Complete bool
}

func PlanTargetTopology(observation TargetTopologyObservation) TargetTopologyPlan {
	previous := normalizeMembers(observation.PreviousMembers)
	configuredPrevious := previous
	if observation.ManifestKnown {
		configuredPrevious = configuredMembers(previous, observation.Configured)
	}
	observed := normalizeMembers(observation.ObservedMembers)

	if len(observed) > 0 {
		members := observed
		if !membersComplete(configuredPrevious, observation.Successful) {
			members = normalizeMembers(append(append([]string{}, configuredPrevious...), observed...))
		}
		return TargetTopologyPlan{Members: members, Complete: membersComplete(members, observation.Successful)}
	}

	if observation.ManifestKnown && len(previous) > 0 && len(configuredPrevious) == 0 {
		return TargetTopologyPlan{Members: nil, Retire: true}
	}
	return TargetTopologyPlan{
		Members:  configuredPrevious,
		Complete: membersComplete(configuredPrevious, observation.Successful),
	}
}

func normalizeMembers(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	slices.Sort(out)
	return out
}

func configuredMembers(values []string, configured map[string]struct{}) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		if _, ok := configured[value]; ok {
			out = append(out, value)
		}
	}
	return out
}

func membersComplete(values []string, successful map[string]bool) bool {
	if len(values) == 0 {
		return false
	}
	for _, value := range values {
		if !successful[value] {
			return false
		}
	}
	return true
}

type RecheckCandidateClass string

const (
	RecheckCandidateMatch             RecheckCandidateClass = "match"
	RecheckCandidateNonmatch          RecheckCandidateClass = "nonmatch"
	RecheckCandidateMissingLabels     RecheckCandidateClass = "missing_labels"
	RecheckCandidateStaleCAS          RecheckCandidateClass = "stale_cas"
	RecheckCandidateIncompleteSources RecheckCandidateClass = "incomplete_sources"
)

func RecheckJobCanRun(jobStatus string, failedPredecessor bool) bool {
	return !failedPredecessor && jobStatus != "failed" && jobStatus != "completed"
}

func ClassifyRecheckCandidate(coverageComplete bool, candidate RecheckCandidateClass) (counter string, promote bool) {
	switch candidate {
	case RecheckCandidateMatch:
		if coverageComplete {
			return "", true
		}
		return "incomplete_sources", false
	case RecheckCandidateMissingLabels:
		return "missing_labels", false
	case RecheckCandidateStaleCAS:
		return "stale_conflicts", false
	case RecheckCandidateIncompleteSources:
		return "incomplete_sources", false
	case RecheckCandidateNonmatch:
		if !coverageComplete {
			return "incomplete_sources", false
		}
		return "ineligible", false
	default:
		return "ineligible", false
	}
}
