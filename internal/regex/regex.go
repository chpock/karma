package regex

import (
	"regexp"
)

func WrapRegexWithAnchors(r string) string {
	return "^(?:" + r + ")$"
}

func MustCompileAnchored(r string) *regexp.Regexp {
	return regexp.MustCompile(WrapRegexWithAnchors(r))
}

func CompileAnchored(r string) (*regexp.Regexp, error) {
	return regexp.Compile(WrapRegexWithAnchors(r))
}
