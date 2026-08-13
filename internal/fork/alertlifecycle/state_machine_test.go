package alertlifecycle

import (
	"reflect"
	"slices"
	"testing"
)

func TestLifecycleStateMachineSequences(t *testing.T) {
	tests := []struct {
		name  string
		steps []func(LifecycleDimensions) LifecycleDimensions
		want  []State
	}{
		{
			name: "active disappears resolves and refires",
			steps: []func(LifecycleDimensions) LifecycleDimensions{
				func(d LifecycleDimensions) LifecycleDimensions { return ObserveLive(d, StateActive) },
				ObserveAbsent,
				func(d LifecycleDimensions) LifecycleDimensions { next, _ := ResolveDimensions(d); return next },
				func(d LifecycleDimensions) LifecycleDimensions { return ObserveLive(d, StateActive) },
			},
			want: []State{StateActive, StateInactive, StateResolved, StateActive},
		},
		{
			name: "suppression remains sticky until unsuppressed refire",
			steps: []func(LifecycleDimensions) LifecycleDimensions{
				func(d LifecycleDimensions) LifecycleDimensions { return ObserveLive(d, StateSuppressed) },
				ObserveAbsent,
				ObserveAbsent,
				func(d LifecycleDimensions) LifecycleDimensions { return ObserveLive(d, StateActive) },
			},
			want: []State{StateSuppressed, StateSuppressed, StateSuppressed, StateActive},
		},
		{
			name: "inactive promotion is sticky and resolve overrides projection",
			steps: []func(LifecycleDimensions) LifecycleDimensions{
				func(d LifecycleDimensions) LifecycleDimensions { return ObserveLive(d, StateActive) },
				ObserveAbsent,
				func(d LifecycleDimensions) LifecycleDimensions { next, _ := PromoteSuppressed(d); return next },
				func(d LifecycleDimensions) LifecycleDimensions { next, _ := ResolveDimensions(d); return next },
			},
			want: []State{StateActive, StateInactive, StateSuppressed, StateResolved},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			state := LifecycleDimensions{RawState: StateUnprocessed}
			got := make([]State, 0, len(tc.steps))
			for _, step := range tc.steps {
				state = step(state)
				got = append(got, state.ProjectedState())
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("states=%v want=%v final=%+v", got, tc.want, state)
			}
		})
	}
}

func TestLifecycleDimensionsExhaustiveStateEventProduct(t *testing.T) {
	rawStates := []State{StateActive, StateSuppressed, StateUnprocessed}
	bools := []bool{false, true}
	for _, raw := range rawStates {
		for _, live := range bools {
			for _, suppressed := range bools {
				for _, resolved := range bools {
					initial := LifecycleDimensions{RawState: raw, IsLive: live, IsSuppressed: suppressed, IsResolved: resolved}
					name := string(raw) + "/live=" + boolName(live) + "/suppressed=" + boolName(suppressed) + "/resolved=" + boolName(resolved)
					t.Run(name, func(t *testing.T) {
						for _, observedRaw := range rawStates {
							next := ObserveLive(initial, observedRaw)
							if !next.IsLive || next.IsResolved || next.IsSuppressed != (observedRaw == StateSuppressed) || next.RawState != observedRaw {
								t.Fatalf("ObserveLive(%s)=%+v", observedRaw, next)
							}
						}
						absent := ObserveAbsent(initial)
						if absent.IsLive || absent.RawState != initial.RawState || absent.IsSuppressed != initial.IsSuppressed || absent.IsResolved != initial.IsResolved {
							t.Fatalf("ObserveAbsent changed sticky dimensions: before=%+v after=%+v", initial, absent)
						}
						resolvedState, resolveApplied := ResolveDimensions(initial)
						wantResolve := !initial.IsLive && !initial.IsResolved
						if resolveApplied != wantResolve || (resolveApplied && !resolvedState.IsResolved) || (!resolveApplied && resolvedState != initial) {
							t.Fatalf("ResolveDimensions=(%+v,%v) wantApplied=%v", resolvedState, resolveApplied, wantResolve)
						}
						promoted, promoteApplied := PromoteSuppressed(initial)
						wantPromote := !initial.IsLive && !initial.IsResolved && !initial.IsSuppressed
						if promoteApplied != wantPromote || (promoteApplied && !promoted.IsSuppressed) || (!promoteApplied && promoted != initial) {
							t.Fatalf("PromoteSuppressed=(%+v,%v) wantApplied=%v", promoted, promoteApplied, wantPromote)
						}
					})
				}
			}
		}
	}
}

func boolName(value bool) string {
	if value {
		return "true"
	}
	return "false"
}

func TestTargetTopologyTwoMemberExhaustiveCoverage(t *testing.T) {
	bools := []bool{false, true}
	for _, manifestKnown := range bools {
		for _, configuredA := range bools {
			for _, configuredB := range bools {
				for _, observedA := range bools {
					for _, observedB := range bools {
						for _, successA := range bools {
							for _, successB := range bools {
								configured := map[string]struct{}{}
								if configuredA {
									configured["a"] = struct{}{}
								}
								if configuredB {
									configured["b"] = struct{}{}
								}
								observed := []string{}
								if observedA {
									observed = append(observed, "a")
								}
								if observedB {
									observed = append(observed, "b")
								}
								plan := PlanTargetTopology(TargetTopologyObservation{
									PreviousMembers: []string{"a", "b"}, ObservedMembers: observed,
									Configured: configured, ManifestKnown: manifestKnown,
									Successful: map[string]bool{"a": successA, "b": successB},
								})
								if plan.Retire != (manifestKnown && !configuredA && !configuredB && len(observed) == 0) {
									t.Fatalf("unexpected retirement: known=%v configured=%v observed=%v plan=%+v", manifestKnown, configured, observed, plan)
								}
								if plan.Complete {
									if len(plan.Members) == 0 {
										t.Fatalf("empty member set cannot be complete: %+v", plan)
									}
									for _, member := range plan.Members {
										if (member == "a" && !successA) || (member == "b" && !successB) {
											t.Fatalf("unsuccessful member marked complete: %+v", plan)
										}
									}
								}
								if manifestKnown && len(observed) == 0 && !plan.Retire {
									for _, member := range plan.Members {
										if _, ok := configured[member]; !ok {
											t.Fatalf("deconfigured member retained: configured=%v plan=%+v", configured, plan)
										}
									}
								}
							}
						}
					}
				}
			}
		}
	}
}

func TestC2SourceStatusTopologyProduct(t *testing.T) {
	statuses := []string{"success", "failed", "invalid", "unknown", "retired"}
	topologies := []struct {
		name       string
		configured map[string]struct{}
		observed   []string
		retire     bool
	}{
		{name: "configured-present", configured: map[string]struct{}{"a": {}, "b": {}}, observed: []string{"a", "b"}},
		{name: "configured-absent", configured: map[string]struct{}{"a": {}, "b": {}}},
		{name: "member-removed", configured: map[string]struct{}{"a": {}}},
		{name: "all-removed", configured: map[string]struct{}{}, retire: true},
		{name: "reassigned-observation", configured: map[string]struct{}{}, observed: []string{"a"}},
	}
	for _, statusA := range statuses {
		for _, statusB := range statuses {
			for _, topology := range topologies {
				t.Run(statusA+"/"+statusB+"/"+topology.name, func(t *testing.T) {
					successful := map[string]bool{
						"a": statusA == "success",
						"b": statusB == "success",
					}
					plan := PlanTargetTopology(TargetTopologyObservation{
						PreviousMembers: []string{"a", "b"},
						ObservedMembers: topology.observed,
						Configured:      topology.configured, ManifestKnown: true,
						Successful: successful,
					})
					if plan.Retire != topology.retire {
						t.Fatalf("retire=%v want=%v plan=%+v", plan.Retire, topology.retire, plan)
					}
					if plan.Complete {
						for _, member := range plan.Members {
							if !successful[member] {
								t.Fatalf("status %q was allowed to confirm coverage: %+v", map[string]string{"a": statusA, "b": statusB}[member], plan)
							}
						}
					}
					if topology.name == "member-removed" && slices.Contains(plan.Members, "b") {
						t.Fatalf("removed member retained: %+v", plan)
					}
					if topology.name == "configured-absent" && plan.Complete != (statusA == "success" && statusB == "success") {
						t.Fatalf("absence completeness=%v statuses=%s/%s", plan.Complete, statusA, statusB)
					}
				})
			}
		}
	}
}

func TestC4RecheckSchedulerCandidateProduct(t *testing.T) {
	jobStates := []string{"queued", "running", "failed", "completed"}
	sourceStatuses := []string{"success", "failed", "invalid", "unknown", "retired"}
	candidates := []RecheckCandidateClass{
		RecheckCandidateMatch, RecheckCandidateNonmatch,
		RecheckCandidateMissingLabels, RecheckCandidateStaleCAS,
		RecheckCandidateIncompleteSources,
	}
	for _, jobState := range jobStates {
		for _, predecessorFailed := range []bool{false, true} {
			for _, sourceStatus := range sourceStatuses {
				for _, candidate := range candidates {
					wantBlocked := predecessorFailed || jobState == "failed"
					canRun := RecheckJobCanRun(jobState, predecessorFailed)
					if canRun != (!wantBlocked && jobState != "completed") {
						t.Fatalf("state=%s predecessor=%v canRun=%v", jobState, predecessorFailed, canRun)
					}
					if !canRun {
						continue
					}
					coverageComplete := sourceStatus == "success" || sourceStatus == "retired"
					counter, promote := ClassifyRecheckCandidate(coverageComplete, candidate)
					if candidate == RecheckCandidateMissingLabels && counter != "missing_labels" {
						t.Fatalf("missing-label candidate counter=%q", counter)
					}
					if candidate == RecheckCandidateStaleCAS && counter != "stale_conflicts" {
						t.Fatalf("stale-CAS candidate counter=%q", counter)
					}
					if candidate != RecheckCandidateMissingLabels && candidate != RecheckCandidateStaleCAS && !coverageComplete && counter != "incomplete_sources" {
						t.Fatalf("incomplete source candidate=%s counter=%q", candidate, counter)
					}
					if promote != (coverageComplete && candidate == RecheckCandidateMatch) {
						t.Fatalf("candidate=%s coverage=%v counter=%s promote=%v", candidate, coverageComplete, counter, promote)
					}
				}
			}
		}
	}
}

func TestTargetTopologyStateMachine(t *testing.T) {
	tests := []struct {
		name string
		in   TargetTopologyObservation
		want TargetTopologyPlan
	}{
		{
			name: "partial HA keeps failed member",
			in: TargetTopologyObservation{
				PreviousMembers: []string{"a", "b"}, ObservedMembers: []string{"a"},
				Configured: map[string]struct{}{"a": {}, "b": {}}, ManifestKnown: true,
				Successful: map[string]bool{"a": true, "b": false},
			},
			want: TargetTopologyPlan{Members: []string{"a", "b"}},
		},
		{
			name: "complete coverage shrinks set",
			in: TargetTopologyObservation{
				PreviousMembers: []string{"a", "b"}, ObservedMembers: []string{"a"},
				Configured: map[string]struct{}{"a": {}, "b": {}}, ManifestKnown: true,
				Successful: map[string]bool{"a": true, "b": true},
			},
			want: TargetTopologyPlan{Members: []string{"a"}, Complete: true},
		},
		{
			name: "deconfigured member does not block absence",
			in: TargetTopologyObservation{
				PreviousMembers: []string{"a", "b"}, Configured: map[string]struct{}{"a": {}}, ManifestKnown: true,
				Successful: map[string]bool{"a": true},
			},
			want: TargetTopologyPlan{Members: []string{"a"}, Complete: true},
		},
		{
			name: "fully removed target retires without remote transition",
			in: TargetTopologyObservation{
				PreviousMembers: []string{"a"}, Configured: map[string]struct{}{}, ManifestKnown: true,
			},
			want: TargetTopologyPlan{Retire: true},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := PlanTargetTopology(tc.in)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("plan=%+v want=%+v", got, tc.want)
			}
		})
	}
}

func FuzzLifecycleDimensionsInvariants(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 4})
	f.Add([]byte{2, 2, 1, 3, 0})
	f.Fuzz(func(t *testing.T, operations []byte) {
		state := ObserveLive(LifecycleDimensions{}, StateActive)
		for _, operation := range operations {
			switch operation % 5 {
			case 0:
				state = ObserveLive(state, StateActive)
			case 1:
				state = ObserveLive(state, StateSuppressed)
			case 2:
				state = ObserveAbsent(state)
			case 3:
				if next, applied := ResolveDimensions(state); applied {
					state = next
				}
			case 4:
				if next, applied := PromoteSuppressed(state); applied {
					state = next
				}
			}
			if state.IsLive && state.IsResolved {
				t.Fatalf("live state cannot be resolved: %+v", state)
			}
			if state.ProjectedState() == StateResolved && (state.IsLive || !state.IsResolved) {
				t.Fatalf("invalid resolved projection: %+v", state)
			}
			if state.ProjectedState() == StateInactive && (state.IsLive || state.IsSuppressed || state.IsResolved) {
				t.Fatalf("invalid inactive projection: %+v", state)
			}
		}
	})
}
