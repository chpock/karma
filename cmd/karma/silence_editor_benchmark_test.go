package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/prymitive/karma/internal/models"
)

func BenchmarkSilenceEditorPreview100k(b *testing.B) {
	matchers, err := compileSilenceEditorMatchers([]silenceEditorMatcher{{
		Name: "service", Value: "api", IsEqual: true,
	}})
	if err != nil {
		b.Fatal(err)
	}
	candidates := make([]silenceEditorPreviewCandidate, 100000)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	for i := range candidates {
		service := "payments"
		if i%2 == 0 {
			service = "api"
		}
		candidates[i] = silenceEditorPreviewCandidate{
			AlertID: fmt.Sprintf("alert-%06d", i), State: models.AlertStateInactive,
			RawState: models.AlertStateActive, LastSeenAt: &now,
			RawLabels: map[string]string{"service": service},
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		matched, tooLarge := matchSilenceEditorPreviewCandidates(matchers, candidates)
		if tooLarge || len(matched) != 50000 {
			b.Fatalf("matched=%d tooLarge=%v", len(matched), tooLarge)
		}
		sortSilenceEditorPreviewAlerts(matched)
	}
}
