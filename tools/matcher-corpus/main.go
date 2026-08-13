package main

import (
	"encoding/json"
	"fmt"
	"os"

	amlabels "github.com/prometheus/alertmanager/pkg/labels"
)

type corpusCase struct {
	Expression string `json:"expression"`
	Valid      bool   `json:"valid"`
	Name       string `json:"name"`
	Value      string `json:"value"`
	IsRegex    bool   `json:"isRegex"`
	IsEqual    bool   `json:"isEqual"`
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: go run . <matcher-corpus.json>")
		os.Exit(2)
	}
	raw, err := os.ReadFile(os.Args[1])
	if err != nil {
		fatal(err)
	}
	var corpus []corpusCase
	if err = json.Unmarshal(raw, &corpus); err != nil {
		fatal(err)
	}
	for _, tc := range corpus {
		matcher, parseErr := amlabels.ParseMatcher(tc.Expression)
		if !tc.Valid {
			if parseErr == nil {
				fatal(fmt.Errorf("%q unexpectedly parsed as %+v", tc.Expression, matcher))
			}
			continue
		}
		if parseErr != nil {
			fatal(fmt.Errorf("%q: %w", tc.Expression, parseErr))
		}
		isRegex := matcher.Type == amlabels.MatchRegexp || matcher.Type == amlabels.MatchNotRegexp
		isEqual := matcher.Type == amlabels.MatchEqual || matcher.Type == amlabels.MatchRegexp
		if matcher.Name != tc.Name || matcher.Value != tc.Value || isRegex != tc.IsRegex || isEqual != tc.IsEqual {
			fatal(fmt.Errorf("%q parsed as name=%q value=%q regex=%v equal=%v", tc.Expression, matcher.Name, matcher.Value, isRegex, isEqual))
		}
	}
	fmt.Printf("validated %d matcher cases against Alertmanager parser\n", len(corpus))
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
