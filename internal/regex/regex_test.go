package regex_test

import (
	"fmt"
	"testing"

	"github.com/prymitive/karma/internal/regex"
)

func TestMustCompileAnchored(t *testing.T) {
	type testCaseT struct {
		in  string
		out string
	}

	testCases := []testCaseT{
		{in: "foo", out: "^(?:foo)$"},
		{in: "^foo", out: "^(?:^foo)$"},
		{in: "foo$", out: "^(?:foo$)$"},
		{in: "^foo$", out: "^(?:^foo$)$"},
		{in: "^^foo$", out: "^(?:^^foo$)$"},
		{in: "foo$$", out: "^(?:foo$$)$"},
		{in: `price\$`, out: `^(?:price\$)$`},
	}

	for _, testCase := range testCases {
		t.Run(fmt.Sprintf("%q => %q", testCase.in, testCase.out), func(t *testing.T) {
			r := regex.MustCompileAnchored(testCase.in)
			if r.String() != testCase.out {
				t.Errorf("Regex mismatch, expected %q got %q", testCase.out, r.String())
			}
		})
	}
}

func TestCompileAnchored(t *testing.T) {
	type testCaseT struct {
		in    string
		out   string
		error bool
	}

	testCases := []testCaseT{
		{in: "foo", out: "^(?:foo)$"},
		{in: "^foo", out: "^(?:^foo)$"},
		{in: "foo$", out: "^(?:foo$)$"},
		{in: "^foo$", out: "^(?:^foo$)$"},
		{in: "^^foo$", out: "^(?:^^foo$)$"},
		{in: "foo$$", out: "^(?:foo$$)$"},
		{in: `price\$`, out: `^(?:price\$)$`},
		{in: ".******", out: "", error: true},
	}

	for _, testCase := range testCases {
		t.Run(fmt.Sprintf("%q => %q", testCase.in, testCase.out), func(t *testing.T) {
			r, err := regex.CompileAnchored(testCase.in)
			hadError := err != nil
			if testCase.error != hadError {
				t.Errorf("CompileAnchored err=%v, expected error=%v", err, testCase.error)
			}
			if err == nil {
				if r.String() != testCase.out {
					t.Errorf("Regex mismatch, expected %q got %q", testCase.out, r.String())
				}
			}
		})
	}
}

func TestCompileAnchoredAlternationIsFullyAnchored(t *testing.T) {
	r, err := regex.CompileAnchored("api|payments")
	if err != nil {
		t.Fatalf("expected regex compile success: %v", err)
	}
	for _, value := range []string{"api", "payments"} {
		if !r.MatchString(value) {
			t.Fatalf("expected %q to match", value)
		}
	}
	for _, value := range []string{"api-v2", "legacy-payments"} {
		if r.MatchString(value) {
			t.Fatalf("expected %q not to match", value)
		}
	}
}

func TestCompileAnchoredEscapedTrailingDollar(t *testing.T) {
	r, err := regex.CompileAnchored(`price\$`)
	if err != nil {
		t.Fatalf("expected regex compile success: %v", err)
	}
	if !r.MatchString("price$") {
		t.Fatalf("expected literal trailing dollar to match")
	}
	if r.MatchString("price") {
		t.Fatalf("expected missing dollar not to match")
	}
}
