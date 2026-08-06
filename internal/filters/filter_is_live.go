package filters

import (
	"strconv"
	"strings"

	"github.com/prymitive/karma/internal/models"
)

type isLiveFilter struct {
	filterBase
	boolValue bool
}

func (filter *isLiveFilter) Value() string {
	return strconv.FormatBool(filter.boolValue)
}

func (filter *isLiveFilter) Match(alert *models.Alert, _ int) (isMatch bool) {
	if alert.HasIsLive {
		isMatch = alert.IsLive == filter.boolValue
	} else {
		isMatch = (len(alert.Alertmanager) > 0) == filter.boolValue
	}
	if isMatch {
		filter.hits++
	}
	return isMatch
}

func newIsLiveFilter(name, operator, rawText, value string) Filter {
	var bv bool
	switch value {
	case trueValue:
		bv = true
	case falseValue:
		bv = false
	default:
		return &filterBase{rawText: rawText}
	}
	// operator is pre-validated by the registry, buildMatcher cannot fail here
	m, _ := buildMatcher(operator, value)
	return &isLiveFilter{
		filterBase: filterBase{
			matcher:              m,
			name:                 name,
			rawText:              rawText,
			value:                value,
			isValid:              true,
			isAlertmanagerFilter: false,
		},
		boolValue: bv,
	}
}

func isLiveAutocomplete(name string, _ []string, _ []models.Alert, dst map[string]models.Autocomplete) {
	for _, val := range []string{trueValue, falseValue} {
		token := name + equalOperator + val
		setAC(dst, token, []string{
			name,
			strings.TrimPrefix(name, "@"),
			name + equalOperator,
			val,
		})
	}
}
