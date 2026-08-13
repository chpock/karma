package models

import (
	"cmp"
	"slices"
	"strconv"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/fvbommel/sortorder"
	"github.com/go-json-experiment/json/jsontext"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/prymitive/karma/internal/config"
)

// AlertState encodes the state of an alert as a compact uint8.
type AlertState uint8

const (
	// AlertStateUnprocessed means that Alertmanager notify didn't yet process it
	// and AM doesn't know if alert is active or suppressed
	AlertStateUnprocessed AlertState = iota
	// AlertStateActive is the state in which we know that the alert should fire
	AlertStateActive
	// AlertStateSuppressed means that we know that alert is silenced or inhibited
	AlertStateSuppressed
	// AlertStateInactive means that the alert is not present in the latest live snapshot
	AlertStateInactive
	// AlertStateResolved means that the alert was manually resolved by a user
	AlertStateResolved
)

// AlertStateList exports all alert states so other packages can get this list
var AlertStateList = []AlertState{
	AlertStateUnprocessed,
	AlertStateActive,
	AlertStateSuppressed,
	AlertStateInactive,
	AlertStateResolved,
}

var alertStateToString = [5]string{"unprocessed", "active", "suppressed", "inactive", "resolved"}

var alertStateFromString = map[string]AlertState{
	"unprocessed": AlertStateUnprocessed,
	"active":      AlertStateActive,
	"suppressed":  AlertStateSuppressed,
	"inactive":    AlertStateInactive,
	"resolved":    AlertStateResolved,
}

func (s AlertState) String() string {
	if int(s) < len(alertStateToString) {
		return alertStateToString[s]
	}
	return "unprocessed"
}

func (s AlertState) MarshalJSON() ([]byte, error) {
	return jsontext.AppendQuote(nil, s.String())
}

func (s *AlertState) UnmarshalJSON(data []byte) error {
	unquoted, err := jsontext.AppendUnquote(nil, data)
	if err != nil {
		return err
	}
	*s = ParseAlertState(string(unquoted))
	return nil
}

// MarshalText implements encoding.TextMarshaler so AlertState can be used as
// a JSON map key.
func (s AlertState) MarshalText() ([]byte, error) {
	return []byte(s.String()), nil
}

// UnmarshalText implements encoding.TextUnmarshaler so AlertState can be used
// as a JSON map key.
func (s *AlertState) UnmarshalText(data []byte) error {
	*s = ParseAlertState(string(data))
	return nil
}

// AlertStateFromString looks up an AlertState by its string representation.
// Returns the state and true if found, or (AlertStateUnprocessed, false) otherwise.
func AlertStateFromString(s string) (AlertState, bool) {
	v, ok := alertStateFromString[s]
	return v, ok
}

// ParseAlertState converts a string to an AlertState.
func ParseAlertState(s string) AlertState {
	if v, ok := alertStateFromString[s]; ok {
		return v
	}
	return AlertStateUnprocessed
}

// OrderedLabel mirrors labels.Label for JSON serialization in the
// [{"name":"...","value":"..."}] format expected by the frontend.
type OrderedLabel struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// OrderedLabels is a slice of OrderedLabel used for JSON serialization.
// It preserves the display order configured via config.Config.Labels.Order.
type OrderedLabels []OrderedLabel

func (ol OrderedLabels) Get(name string) string {
	for _, l := range ol {
		if l.Name == name {
			return l.Value
		}
	}
	return ""
}

func (ol OrderedLabels) MarshalJSONTo(enc *jsontext.Encoder) error {
	w := jsonWriter{enc: enc}
	w.beginArray()
	for _, l := range ol {
		w.beginObject()
		w.key("name")
		w.str(l.Name)
		w.key("value")
		w.str(l.Value)
		w.endObject()
	}
	w.endArray()
	return w.err
}

// LabelsToOrderedLabels converts prometheus Labels to OrderedLabels sorted
// by the configured display order.
func LabelsToOrderedLabels(ls labels.Labels) OrderedLabels {
	dl := make(OrderedLabels, 0, ls.Len())
	ls.Range(func(l labels.Label) {
		dl = append(dl, OrderedLabel{Name: l.Name, Value: l.Value})
	})
	slices.SortFunc(dl, CompareOrderedLabels)
	return dl
}

// CompareOrderedLabels sorts display labels by the configured label order,
// then by name, then by value using natural sort.
func CompareOrderedLabels(a, b OrderedLabel) int {
	ai, bi := -1, -1
	for index, name := range config.Config.Labels.Order {
		if a.Name == name {
			ai = index
		} else if b.Name == name {
			bi = index
		}
		if ai >= 0 && bi >= 0 {
			return cmp.Compare(ai, bi)
		}
	}
	if ai != bi {
		return cmp.Compare(bi, ai)
	}
	if a.Name == b.Name {
		if sortorder.NaturalLess(a.Value, b.Value) {
			return -1
		}
		if sortorder.NaturalLess(b.Value, a.Value) {
			return 1
		}
		return 0
	}
	if sortorder.NaturalLess(a.Name, b.Name) {
		return -1
	}
	if sortorder.NaturalLess(b.Name, a.Name) {
		return 1
	}
	return 0
}

// LabelsFromMap creates a Labels from a map, adding only keys not already present.
func LabelsFromMap(m map[string]string) labels.Labels {
	return labels.FromMap(m)
}

// LabelsSetIfMissing returns a new Labels with the given name/value added only if
// the name is not already present.
func LabelsSetIfMissing(ls labels.Labels, name, value string) labels.Labels {
	if ls.Has(name) {
		return ls
	}
	b := labels.NewBuilder(ls)
	b.Set(name, value)
	return b.Labels()
}

// Alert is vanilla alert + some additional attributes
// karma extends an alert object with:
//   - Links map, it's generated from annotations if annotation value is an url
//     it's pulled out of annotation map and returned under links field,
//     karma UI used this to show links differently than other annotations
type Alert struct {
	StartsAt          time.Time `json:"startsAt"`
	Fingerprint       string    `json:"-"`
	GeneratorURL      string    `json:"-"`
	Receiver          string    `json:"receiver"`
	LabelsFP          string    `json:"id"`
	contentFP         string
	Labels            labels.Labels          `json:"-"`
	Annotations       Annotations            `json:"annotations"`
	SilencedBy        []string               `json:"-"`
	InhibitedBy       []string               `json:"-"`
	Alertmanager      []AlertmanagerInstance `json:"alertmanager"`
	SuppressionCauses []SuppressionCause     `json:"suppressionCauses"`
	State             AlertState             `json:"state"`
	RawState          AlertState             `json:"-"`
	HasRawState       bool                   `json:"-"`
	IsLive            bool                   `json:"-"`
	HasIsLive         bool                   `json:"-"`
	IsResolved        bool                   `json:"-"`
}

// APIAlert is the JSON-serializable representation of Alert.
// Labels are converted to OrderedLabels for the frontend.
type APIAlert struct {
	StartsAt          time.Time              `json:"startsAt"`
	State             string                 `json:"state"`
	RawState          string                 `json:"rawState"`
	IsLive            bool                   `json:"isLive"`
	IsResolved        bool                   `json:"isResolved"`
	Receiver          string                 `json:"receiver"`
	LabelsFP          string                 `json:"id"`
	Annotations       Annotations            `json:"annotations"`
	Labels            OrderedLabels          `json:"labels"`
	Alertmanager      []AlertmanagerInstance `json:"alertmanager"`
	SuppressionCauses []SuppressionCause     `json:"suppressionCauses"`
}

type SuppressionCause struct {
	Target           string                    `json:"target"`
	Source           string                    `json:"source"`
	SilenceID        string                    `json:"silenceID"`
	ResolutionStatus string                    `json:"resolutionStatus"`
	StartsAt         *time.Time                `json:"startsAt"`
	EndsAt           *time.Time                `json:"endsAt"`
	CreatedBy        string                    `json:"createdBy"`
	Comment          string                    `json:"comment"`
	Matchers         []SuppressionCauseMatcher `json:"matchers"`
}

// SuppressionCauseMatcher is immutable display evidence, not an executable matcher.
type SuppressionCauseMatcher struct {
	Name    string `json:"name"`
	Value   string `json:"value"`
	IsRegex bool   `json:"isRegex"`
	IsEqual bool   `json:"isEqual"`
}

func (a APIAlert) MarshalJSONTo(enc *jsontext.Encoder) error {
	w := jsonWriter{enc: enc}
	a.marshalTo(&w)
	return w.err
}

func (a *APIAlert) marshalTo(w *jsonWriter) {
	w.beginObject()
	w.key("startsAt")
	w.time(a.StartsAt)
	w.key("state")
	w.str(a.State)
	w.key("rawState")
	w.str(a.RawState)
	w.key("isLive")
	w.boolean(a.IsLive)
	w.key("isResolved")
	w.boolean(a.IsResolved)
	w.key("receiver")
	w.str(a.Receiver)
	w.key("id")
	w.str(a.LabelsFP)
	w.key("annotations")
	w.beginArray()
	for i := range a.Annotations {
		a.Annotations[i].marshalTo(w)
	}
	w.endArray()
	w.key("labels")
	w.beginArray()
	for _, l := range a.Labels {
		w.beginObject()
		w.key("name")
		w.str(l.Name)
		w.key("value")
		w.str(l.Value)
		w.endObject()
	}
	w.endArray()
	w.key("alertmanager")
	w.beginArray()
	for i := range a.Alertmanager {
		a.Alertmanager[i].marshalTo(w)
	}
	w.endArray()
	w.key("suppressionCauses")
	w.beginArray()
	for i := range a.SuppressionCauses {
		cause := &a.SuppressionCauses[i]
		w.beginObject()
		w.key("target")
		w.str(cause.Target)
		w.key("source")
		w.str(cause.Source)
		w.key("silenceID")
		w.str(cause.SilenceID)
		w.key("resolutionStatus")
		w.str(cause.ResolutionStatus)
		w.key("startsAt")
		if cause.StartsAt == nil {
			w.null()
		} else {
			w.time(*cause.StartsAt)
		}
		w.key("endsAt")
		if cause.EndsAt == nil {
			w.null()
		} else {
			w.time(*cause.EndsAt)
		}
		w.key("createdBy")
		w.str(cause.CreatedBy)
		w.key("comment")
		w.str(cause.Comment)
		w.key("matchers")
		w.beginArray()
		for matcherIndex := range cause.Matchers {
			matcher := &cause.Matchers[matcherIndex]
			w.beginObject()
			w.key("name")
			w.str(matcher.Name)
			w.key("value")
			w.str(matcher.Value)
			w.key("isRegex")
			w.boolean(matcher.IsRegex)
			w.key("isEqual")
			w.boolean(matcher.IsEqual)
			w.endObject()
		}
		w.endArray()
		w.endObject()
	}
	w.endArray()
	w.endObject()
}

var seps = []byte{'\xff'}

// UpdateFingerprints will generate a new set of fingerprints for this alert
func (a *Alert) UpdateFingerprints() {
	labelsHash := a.Labels.Hash()
	a.LabelsFP = strconv.FormatUint(labelsHash, 16)

	h := xxhash.New()
	for _, a := range a.Annotations {
		_, _ = h.WriteString(a.Name)
		_, _ = h.Write(seps)
		_, _ = h.WriteString(a.Value)
		_, _ = h.Write(seps)
		_, _ = h.WriteString(strconv.FormatBool(a.IsAction))
		_, _ = h.Write(seps)
		_, _ = h.WriteString(strconv.FormatBool(a.IsLink))
		_, _ = h.Write(seps)
		_, _ = h.WriteString(strconv.FormatBool(a.Visible))
		_, _ = h.Write(seps)

	}
	_, _ = h.WriteString(strconv.FormatUint(labelsHash, 16))
	_, _ = h.Write(seps)
	_, _ = h.WriteString(a.StartsAt.Format(time.RFC3339))
	_, _ = h.WriteString(a.State.String())
	rawState := a.RawState
	if !a.HasRawState {
		rawState = a.State
	}
	isLive := a.IsLive
	if !a.HasIsLive {
		if a.State == AlertStateInactive || a.State == AlertStateResolved {
			isLive = false
		} else if len(a.Alertmanager) > 0 {
			isLive = true
		}
	}
	isResolved := a.IsResolved
	if a.State == AlertStateResolved {
		isResolved = true
	}
	_, _ = h.Write(seps)
	_, _ = h.WriteString(rawState.String())
	_, _ = h.Write(seps)
	_, _ = h.WriteString(strconv.FormatBool(isLive))
	_, _ = h.Write(seps)
	_, _ = h.WriteString(strconv.FormatBool(isResolved))
	for _, am := range a.Alertmanager {
		_, _ = h.WriteString(am.Fingerprint)
		_, _ = h.Write(seps)
		_, _ = h.WriteString(am.Name)
		_, _ = h.Write(seps)
		_, _ = h.WriteString(am.Cluster)
		_, _ = h.Write(seps)
		_, _ = h.WriteString(am.State.String())
		_, _ = h.Write(seps)
		_, _ = h.WriteString(am.StartsAt.Format(time.RFC3339))
		_, _ = h.Write(seps)
		_, _ = h.WriteString(am.Source)
		_, _ = h.Write(seps)
		for _, s := range am.SilencedBy {
			_, _ = h.WriteString(s)
			_, _ = h.Write(seps)
		}
		for _, s := range am.InhibitedBy {
			_, _ = h.WriteString(s)
			_, _ = h.Write(seps)
		}
	}
	for _, cause := range a.SuppressionCauses {
		_, _ = h.WriteString(cause.Target)
		_, _ = h.Write(seps)
		_, _ = h.WriteString(cause.Source)
		_, _ = h.Write(seps)
		_, _ = h.WriteString(cause.SilenceID)
		_, _ = h.Write(seps)
		_, _ = h.WriteString(cause.ResolutionStatus)
		_, _ = h.Write(seps)
		if cause.StartsAt != nil {
			_, _ = h.WriteString(cause.StartsAt.Format(time.RFC3339Nano))
		}
		_, _ = h.Write(seps)
		if cause.EndsAt != nil {
			_, _ = h.WriteString(cause.EndsAt.Format(time.RFC3339Nano))
		}
		_, _ = h.Write(seps)
		_, _ = h.WriteString(cause.CreatedBy)
		_, _ = h.Write(seps)
		_, _ = h.WriteString(cause.Comment)
		_, _ = h.Write(seps)
		for _, matcher := range cause.Matchers {
			_, _ = h.WriteString(matcher.Name)
			_, _ = h.Write(seps)
			_, _ = h.WriteString(matcher.Value)
			_, _ = h.Write(seps)
			_, _ = h.WriteString(strconv.FormatBool(matcher.IsRegex))
			_, _ = h.Write(seps)
			_, _ = h.WriteString(strconv.FormatBool(matcher.IsEqual))
			_, _ = h.Write(seps)
		}
	}
	_, _ = h.WriteString(a.Receiver)
	a.contentFP = strconv.FormatUint(h.Sum64(), 16)
}

// LabelsFingerprint is a checksum computed only from labels which should be
// unique for every alert
func (a *Alert) LabelsFingerprint() string {
	return a.LabelsFP
}

// ContentFingerprint is a checksum computed from entire alert object
func (a *Alert) ContentFingerprint() string {
	return a.contentFP
}
