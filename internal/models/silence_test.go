package models_test

import (
	"fmt"
	"testing"

	"github.com/prymitive/karma/internal/models"
)

func TestSilenceIsMatch(t *testing.T) {
	type testCaseT struct {
		labels  map[string]string
		silence models.Silence
		isMatch bool
	}

	testCases := []testCaseT{
		{
			silence: models.Silence{},
			labels:  map[string]string{},
			isMatch: true,
		},
		{
			silence: models.Silence{
				Matchers: []models.SilenceMatcher{
					models.NewSilenceMatcher("job", "foo", false, true),
				},
			},
			labels:  map[string]string{},
			isMatch: false,
		},
		{
			silence: models.Silence{
				Matchers: []models.SilenceMatcher{
					models.NewSilenceMatcher("job", "foo", false, true),
				},
			},
			labels:  map[string]string{"job": "foo"},
			isMatch: true,
		},
		{
			silence: models.Silence{
				Matchers: []models.SilenceMatcher{
					models.NewSilenceMatcher("job", "foo", false, true),
				},
			},
			labels:  map[string]string{"job": "bar"},
			isMatch: false,
		},
		{
			silence: models.Silence{
				Matchers: []models.SilenceMatcher{
					models.NewSilenceMatcher("job", "foo", false, false),
				},
			},
			labels:  map[string]string{},
			isMatch: true,
		},
		{
			silence: models.Silence{
				Matchers: []models.SilenceMatcher{
					models.NewSilenceMatcher("job", "", false, false),
				},
			},
			labels:  map[string]string{},
			isMatch: false,
		},
		{
			silence: models.Silence{
				Matchers: []models.SilenceMatcher{
					models.NewSilenceMatcher("job", ".*", true, false),
				},
			},
			labels:  map[string]string{},
			isMatch: false,
		},
		{
			silence: models.Silence{
				Matchers: []models.SilenceMatcher{
					models.NewSilenceMatcher("job", ".*", true, true),
				},
			},
			labels:  map[string]string{},
			isMatch: true,
		},
		{
			silence: models.Silence{
				Matchers: []models.SilenceMatcher{
					models.NewSilenceMatcher("job", "foo", false, true),
					models.NewSilenceMatcher("instance", "foo", false, true),
				},
			},
			labels:  map[string]string{"job": "bar"},
			isMatch: false,
		},
		{
			silence: models.Silence{
				Matchers: []models.SilenceMatcher{
					models.NewSilenceMatcher("job", "foo", false, true),
					models.NewSilenceMatcher("instance", "foo", false, true),
				},
			},
			labels:  map[string]string{"job": "bar", "instance": "bar"},
			isMatch: false,
		},
		{
			silence: models.Silence{
				Matchers: []models.SilenceMatcher{
					models.NewSilenceMatcher("job", "foo", false, true),
					models.NewSilenceMatcher("instance", "foo", false, true),
				},
			},
			labels:  map[string]string{"job": "foo", "instance": "foo"},
			isMatch: true,
		},
		{
			silence: models.Silence{
				Matchers: []models.SilenceMatcher{
					models.NewSilenceMatcher("job", "foo", false, true),
					models.NewSilenceMatcher("instance", "foo", false, false),
				},
			},
			labels:  map[string]string{"job": "bar"},
			isMatch: false,
		},
		{
			silence: models.Silence{
				Matchers: []models.SilenceMatcher{
					models.NewSilenceMatcher("job", "foo", false, true),
					models.NewSilenceMatcher("instance", "f.*", true, false),
				},
			},
			labels:  map[string]string{"job": "bar", "instance": "fa"},
			isMatch: false,
		},
		{
			silence: models.Silence{
				Matchers: []models.SilenceMatcher{
					models.NewSilenceMatcher("job", "foo", false, false),
					models.NewSilenceMatcher("instance", "f.*", true, true),
				},
			},
			labels:  map[string]string{"job": "bar", "instance": "fa"},
			isMatch: true,
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("matchers=%v ? labels=%v", tc.silence.Matchers, tc.labels), func(t *testing.T) {
			isMatch := tc.silence.IsMatch(tc.labels)
			if isMatch != tc.isMatch {
				t.Errorf("Silence.IsMatch() returned %v, expected %v, matchers=%v labels=%v",
					isMatch, tc.isMatch, tc.silence.Matchers, tc.labels)
			}
		})
	}
}

func TestIsKnownSilenceStatus(t *testing.T) {
	tests := []struct {
		status string
		want   bool
	}{
		{status: "pending", want: true},
		{status: "active", want: true},
		{status: "expired", want: true},
		{status: "", want: false},
		{status: "future-state", want: false},
		{status: " active ", want: false},
	}
	for _, tc := range tests {
		if got := models.IsKnownSilenceStatus(tc.status); got != tc.want {
			t.Errorf("IsKnownSilenceStatus(%q)=%t, want %t", tc.status, got, tc.want)
		}
	}
}
