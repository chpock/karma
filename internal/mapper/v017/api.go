package v017

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	json "github.com/go-json-experiment/json"
	"github.com/go-json-experiment/json/jsontext"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/prymitive/karma/internal/mapper"
	"github.com/prymitive/karma/internal/models"
	internalRegex "github.com/prymitive/karma/internal/regex"
)

var silenceMatcherNamePattern = regexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:]*$`)

// dateTime is a time.Time that marshals to RFC3339 with millisecond precision,
// matching the format used by Alertmanager's API.
type dateTime time.Time

func (dt dateTime) MarshalJSONTo(enc *jsontext.Encoder) error {
	return enc.WriteToken(jsontext.String(time.Time(dt).Format("2006-01-02T15:04:05.000Z07:00")))
}

func (dt *dateTime) UnmarshalJSONFrom(dec *jsontext.Decoder) error {
	val, err := dec.ReadValue()
	if err != nil {
		return err
	}
	s, err := jsontext.AppendUnquote(nil, val)
	if err != nil {
		return err
	}
	t, err := time.Parse(time.RFC3339Nano, string(s))
	if err != nil {
		return err
	}
	*dt = dateTime(t)
	return nil
}

type receiver struct {
	Name string `json:"name"`
}

type alertStatus struct {
	State       string   `json:"state"`
	InhibitedBy []string `json:"inhibitedBy"`
	SilencedBy  []string `json:"silencedBy"`
}

type jsonLabels struct {
	ls labels.Labels
}

func (jl *jsonLabels) UnmarshalJSONFrom(dec *jsontext.Decoder) error {
	if _, err := dec.ReadToken(); err != nil {
		return err
	}

	var ls []labels.Label
	for dec.PeekKind() != '}' {
		keyTok, err := dec.ReadToken()
		if err != nil {
			return err
		}
		name := keyTok.String()
		valTok, err := dec.ReadToken()
		if err != nil {
			return err
		}
		ls = append(ls, labels.Label{
			Name:  name,
			Value: valTok.String(),
		})
	}

	if _, err := dec.ReadToken(); err != nil {
		return err
	}

	jl.ls = labels.New(ls...)
	return nil
}

type jsonAnnotations struct {
	annos models.Annotations
}

func (ja *jsonAnnotations) UnmarshalJSONFrom(dec *jsontext.Decoder) error {
	if _, err := dec.ReadToken(); err != nil {
		return err
	}

	var annotations models.Annotations
	for dec.PeekKind() != '}' {
		keyTok, err := dec.ReadToken()
		if err != nil {
			return err
		}
		name := keyTok.String()
		valTok, err := dec.ReadToken()
		if err != nil {
			return err
		}
		annotations = append(annotations, models.NewAnnotation(name, valTok.String()))
	}

	if _, err := dec.ReadToken(); err != nil {
		return err
	}

	models.SortAnnotations(annotations)
	ja.annos = annotations
	return nil
}

type alert struct {
	StartsAt     time.Time       `json:"startsAt"`
	Annotations  jsonAnnotations `json:"annotations"`
	Labels       jsonLabels      `json:"labels"`
	Fingerprint  string          `json:"fingerprint"`
	GeneratorURL string          `json:"generatorURL"`
	Status       alertStatus     `json:"status"`
}

type alertGroup struct {
	Labels   jsonLabels `json:"labels"`
	Receiver receiver   `json:"receiver"`
	Alerts   []alert    `json:"alerts"`
}

type matcher struct {
	IsEqual *bool   `json:"isEqual,omitempty"`
	Name    *string `json:"name"`
	Value   *string `json:"value"`
	IsRegex *bool   `json:"isRegex"`
}

type silence struct {
	EndsAt      *dateTime         `json:"endsAt"`
	StartsAt    *dateTime         `json:"startsAt"`
	CreatedAt   *dateTime         `json:"createdAt,omitempty"`
	UpdatedAt   *dateTime         `json:"updatedAt,omitempty"`
	ID          *string           `json:"id,omitempty"`
	Comment     *string           `json:"comment"`
	CreatedBy   *string           `json:"createdBy"`
	Matchers    *[]matcher        `json:"matchers"`
	Annotations map[string]string `json:"annotations,omitempty"`
	Status      *struct {
		State string `json:"state"`
	} `json:"status,omitempty"`
}

func newHTTPClient(uri string, headers map[string]string, httpTransport http.RoundTripper) (*http.Client, *url.URL) {
	u, _ := url.Parse(uri)

	transport := http.DefaultTransport
	if httpTransport != nil {
		transport = httpTransport
	}
	transport = mapper.SetHeaders(transport, headers)

	if u.User.Username() != "" {
		username := u.User.Username()
		password, _ := u.User.Password()
		transport = mapper.SetAuth(transport, username, password)
	}

	return &http.Client{Transport: transport}, u
}

func apiGet(ctx context.Context, client *http.Client, baseURL *url.URL, apiPath string) (io.ReadCloser, error) {
	u := *baseURL
	u.Path = path.Join(u.Path, "api/v2", apiPath)
	u.User = nil

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request for %s: %w", u.String(), err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("request to %s returned status %d", u.String(), resp.StatusCode)
	}

	return resp.Body, nil
}

func groups(client *http.Client, baseURL *url.URL, timeout time.Duration) ([]models.AlertGroup, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	body, err := apiGet(ctx, client, baseURL, "alerts/groups")
	if err != nil {
		return []models.AlertGroup{}, err
	}
	defer body.Close()

	var alertGroups []alertGroup
	if err := json.UnmarshalRead(body, &alertGroups); err != nil {
		return []models.AlertGroup{}, fmt.Errorf("failed to decode alert groups: %w", err)
	}

	ret := make([]models.AlertGroup, 0, len(alertGroups))

	for _, group := range alertGroups {
		g := models.AlertGroup{
			Receiver: group.Receiver.Name,
			Labels:   group.Labels.ls,
			Alerts:   make(models.AlertList, 0, len(group.Alerts)),
		}
		for _, alert := range group.Alerts {
			a := models.Alert{
				Fingerprint:  alert.Fingerprint,
				Receiver:     group.Receiver.Name,
				Annotations:  alert.Annotations.annos,
				Labels:       alert.Labels.ls,
				StartsAt:     alert.StartsAt,
				GeneratorURL: alert.GeneratorURL,
				State:        models.ParseAlertState(alert.Status.State),
				InhibitedBy:  alert.Status.InhibitedBy,
				SilencedBy:   alert.Status.SilencedBy,
			}
			sort.Strings(a.InhibitedBy)
			sort.Strings(a.SilencedBy)
			a.UpdateFingerprints()
			g.Alerts = append(g.Alerts, a)
		}
		ret = append(ret, g)
	}

	return ret, nil
}

func silences(client *http.Client, baseURL *url.URL, timeout time.Duration) ([]models.Silence, []models.SilenceWarning, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	body, err := apiGet(ctx, client, baseURL, "silences")
	if err != nil {
		return []models.Silence{}, nil, err
	}
	defer body.Close()

	entries, err := readSilenceResponseEntries(body)
	if err != nil {
		return []models.Silence{}, nil, fmt.Errorf("failed to decode silences: %w", err)
	}

	ret := make([]models.Silence, 0, len(entries))
	warnings := make([]models.SilenceWarning, 0)
	type validEntry struct {
		index   int
		silence models.Silence
	}
	valid := make([]validEntry, 0, len(entries))
	counts := map[string]int{}
	for index, entry := range entries {
		mapped, warning := decodeSilenceResponseEntry(index, entry)
		if warning != nil {
			warnings = append(warnings, *warning)
			continue
		}
		counts[mapped.ID]++
		valid = append(valid, validEntry{index: index, silence: *mapped})
	}
	for _, entry := range valid {
		if counts[entry.silence.ID] > 1 {
			warnings = append(warnings, newSilenceWarning(entry.index, entry.silence.ID, entry.silence.StatusState, "id", "duplicate_id", "silence id occurs more than once in the response"))
			continue
		}
		ret = append(ret, entry.silence)
	}
	sort.SliceStable(warnings, func(i, j int) bool { return warnings[i].EntryIndex < warnings[j].EntryIndex })

	return ret, warnings, nil
}

func readSilenceResponseEntries(body io.Reader) ([]jsontext.Value, error) {
	decoder := jsontext.NewDecoder(body, jsontext.AllowDuplicateNames(true), jsontext.AllowInvalidUTF8(true))
	token, err := decoder.ReadToken()
	if err != nil {
		return nil, err
	}
	if token.Kind() != '[' {
		return nil, errors.New("top-level silence response must be an array")
	}
	entries := make([]jsontext.Value, 0)
	for decoder.PeekKind() != ']' {
		entry, readErr := decoder.ReadValue()
		if readErr != nil {
			return nil, readErr
		}
		entries = append(entries, entry.Clone())
	}
	if _, err = decoder.ReadToken(); err != nil {
		return nil, err
	}
	if _, err = decoder.ReadToken(); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, errors.New("silence response contains data after the array")
		}
		return nil, err
	}
	return entries, nil
}

func decodeSilenceResponseEntry(index int, raw jsontext.Value) (*models.Silence, *models.SilenceWarning) {
	if raw.Kind() != '{' {
		warning := newSilenceWarning(index, "", "missing", "$", "wrong_type", "silence entry must be an object")
		return nil, &warning
	}
	if !utf8.Valid(raw) {
		warning := newSilenceWarning(index, "", "missing", "$", "invalid_utf8", "silence entry contains invalid UTF-8")
		return nil, &warning
	}
	if shapeErr := validateSilenceResponseShape(raw); shapeErr != nil {
		warning := newSilenceWarning(index, extractSilenceString(raw, "id"), extractSilenceStatus(raw), shapeErr.field, shapeErr.reasonCode, shapeErr.message)
		return nil, &warning
	}
	var s silence
	if err := json.Unmarshal(raw, &s); err != nil {
		field, reason := silenceDecodeError(err)
		warning := newSilenceWarning(index, extractSilenceString(raw, "id"), extractSilenceStatus(raw), field, reason, "silence entry contains an invalid field")
		return nil, &warning
	}
	mapped, validationErr := mapSilenceResponse(s)
	if validationErr != nil {
		warning := newSilenceWarning(index, stringValue(s.ID), silenceStatusValue(s), validationErr.field, validationErr.reasonCode, validationErr.message)
		return nil, &warning
	}
	return mapped, nil
}

func validateSilenceResponseShape(raw jsontext.Value) *silenceValidationError {
	var object map[string]jsontext.Value
	if err := json.Unmarshal(raw, &object); err != nil {
		if errors.Is(err, jsontext.ErrDuplicateName) {
			var syntacticErr *jsontext.SyntacticError
			field := "$"
			if errors.As(err, &syntacticErr) {
				field = jsonPointerToSilenceField(string(syntacticErr.JSONPointer))
			}
			return invalidSilenceField(field, "duplicate_field", "silence entry contains a duplicate field")
		}
		return invalidSilenceField("$", "invalid_entry", "silence entry object is invalid")
	}
	expectedKinds := []struct {
		field string
		kind  jsontext.Kind
	}{
		{field: "id", kind: '"'},
		{field: "status", kind: '{'},
		{field: "startsAt", kind: '"'},
		{field: "endsAt", kind: '"'},
		{field: "createdAt", kind: '"'},
		{field: "updatedAt", kind: '"'},
		{field: "createdBy", kind: '"'},
		{field: "comment", kind: '"'},
		{field: "matchers", kind: '['},
		{field: "annotations", kind: '{'},
	}
	for _, expected := range expectedKinds {
		if value, found := object[expected.field]; found && value.Kind() != expected.kind {
			return invalidSilenceField(expected.field, "wrong_type", "silence field has an unexpected type")
		}
	}
	if rawStatus, found := object["status"]; found {
		var status map[string]jsontext.Value
		if err := json.Unmarshal(rawStatus, &status); err != nil {
			return invalidSilenceField("status", "invalid_entry", "silence status object is invalid")
		}
		if state, stateFound := status["state"]; stateFound && state.Kind() != '"' {
			return invalidSilenceField("status.state", "wrong_type", "silence status.state has an unexpected type")
		}
	}
	if rawAnnotations, found := object["annotations"]; found {
		var annotations map[string]jsontext.Value
		if err := json.Unmarshal(rawAnnotations, &annotations); err != nil {
			return invalidSilenceField("annotations", "invalid_entry", "silence annotations object is invalid")
		}
		annotationNames := make([]string, 0, len(annotations))
		for name := range annotations {
			annotationNames = append(annotationNames, name)
		}
		sort.Strings(annotationNames)
		for _, name := range annotationNames {
			value := annotations[name]
			if value.Kind() != '"' {
				return invalidSilenceField("annotations."+name, "wrong_type", "silence annotation value has an unexpected type")
			}
		}
	}
	if rawMatchers, found := object["matchers"]; found {
		var matchers []jsontext.Value
		if err := json.Unmarshal(rawMatchers, &matchers); err != nil {
			return invalidSilenceField("matchers", "invalid_entry", "silence matchers array is invalid")
		}
		for index, rawMatcher := range matchers {
			prefix := fmt.Sprintf("matchers[%d]", index)
			if rawMatcher.Kind() != '{' {
				return invalidSilenceField(prefix, "wrong_type", "silence matcher must be an object")
			}
			var matcherObject map[string]jsontext.Value
			if err := json.Unmarshal(rawMatcher, &matcherObject); err != nil {
				return invalidSilenceField(prefix, "invalid_entry", "silence matcher object is invalid")
			}
			matcherKinds := []struct {
				field string
				kind  jsontext.Kind
			}{{field: "name", kind: '"'}, {field: "value", kind: '"'}, {field: "isRegex", kind: 't'}, {field: "isEqual", kind: 't'}}
			for _, expected := range matcherKinds {
				if value, fieldFound := matcherObject[expected.field]; fieldFound {
					kind := value.Kind()
					if expected.kind == 't' {
						if kind != 't' && kind != 'f' {
							return invalidSilenceField(prefix+"."+expected.field, "wrong_type", "silence matcher field has an unexpected type")
						}
					} else if kind != expected.kind {
						return invalidSilenceField(prefix+"."+expected.field, "wrong_type", "silence matcher field has an unexpected type")
					}
				}
			}
		}
	}
	return nil
}

type silenceValidationError struct {
	field      string
	reasonCode string
	message    string
}

func newSilenceWarning(index int, silenceID, status, field, reasonCode, message string) models.SilenceWarning {
	return models.SilenceWarning{EntryIndex: index, SilenceID: silenceID, Status: status, Field: field, ReasonCode: reasonCode, Message: message}
}

func rewriteSilenceUsername(body []byte, username string) ([]byte, error) {
	var s silence
	if err := json.Unmarshal(body, &s); err != nil {
		return nil, err
	}
	s.CreatedBy = &username
	return json.Marshal(s)
}

func unmarshal(body []byte) (*models.Silence, error) {
	var s silence
	if err := json.Unmarshal(body, &s); err != nil {
		return nil, err
	}

	return mapSilence(s)
}

func unmarshalResponse(body []byte) (*models.Silence, error) {
	var s silence
	if err := json.Unmarshal(body, &s); err != nil {
		return nil, err
	}

	mapped, validationErr := mapSilenceResponse(s)
	if validationErr != nil {
		return nil, validationErr
	}
	return mapped, nil
}

func mapSilenceResponse(s silence) (*models.Silence, *silenceValidationError) {
	if validationErr := validateSilenceResponse(s); validationErr != nil {
		return nil, validationErr
	}
	mapped, err := mapSilence(s)
	if err != nil {
		return nil, invalidSilenceField("matchers", "invalid_regex", "matcher regular expression is invalid")
	}
	return mapped, nil
}

func mapSilence(s silence) (*models.Silence, error) {
	us := models.Silence{
		ID:        stringValue(s.ID),
		CreatedBy: stringValue(s.CreatedBy),
		Comment:   stringValue(s.Comment),
	}
	if s.StartsAt != nil {
		us.StartsAt = time.Time(*s.StartsAt)
	}
	if s.EndsAt != nil {
		us.EndsAt = time.Time(*s.EndsAt)
	}
	if s.Status != nil {
		us.StatusState = s.Status.State
	}
	if s.CreatedAt != nil {
		us.CreatedAt = time.Time(*s.CreatedAt)
	}
	if s.UpdatedAt != nil {
		updatedAt := time.Time(*s.UpdatedAt)
		us.UpdatedAt = &updatedAt
	}
	if len(s.Annotations) > 0 {
		us.Annotations = make(models.Annotations, 0, len(s.Annotations))
		for name, value := range s.Annotations {
			us.Annotations = append(us.Annotations, models.NewAnnotation(name, value))
		}
		models.SortAnnotations(us.Annotations)
	}

	if s.Matchers == nil {
		return &us, nil
	}
	for index, m := range *s.Matchers {
		isEqual := true
		if m.IsEqual != nil {
			isEqual = *m.IsEqual
		}
		matcher, err := models.NewSilenceMatcherChecked(stringValue(m.Name), stringValue(m.Value), boolValue(m.IsRegex), isEqual)
		if err != nil {
			return nil, fmt.Errorf("invalid matcher %d: %w", index, err)
		}
		us.Matchers = append(us.Matchers, matcher)
	}

	return &us, nil
}

func (e *silenceValidationError) Error() string {
	return e.message
}

func validateSilenceResponse(s silence) *silenceValidationError {
	if s.ID == nil {
		return invalidSilenceField("id", "missing_required_field", "silence id is required")
	}
	if strings.TrimSpace(*s.ID) == "" {
		return invalidSilenceField("id", "empty_required_field", "silence id cannot be empty")
	}
	if s.Status == nil || s.Status.State == "" {
		return invalidSilenceField("status.state", "missing_required_field", "silence status.state is required")
	}
	if !models.IsKnownSilenceStatus(s.Status.State) {
		return invalidSilenceField("status.state", "unsupported_value", "silence status.state is unsupported")
	}
	if s.UpdatedAt != nil && time.Time(*s.UpdatedAt).IsZero() {
		return invalidSilenceField("updatedAt", "invalid_value", "silence updatedAt cannot be zero")
	}
	if s.StartsAt == nil {
		return invalidSilenceField("startsAt", "missing_required_field", "silence startsAt is required")
	}
	if time.Time(*s.StartsAt).IsZero() {
		return invalidSilenceField("startsAt", "invalid_value", "silence startsAt cannot be zero")
	}
	if s.EndsAt == nil {
		return invalidSilenceField("endsAt", "missing_required_field", "silence endsAt is required")
	}
	if time.Time(*s.EndsAt).IsZero() {
		return invalidSilenceField("endsAt", "invalid_value", "silence endsAt cannot be zero")
	}
	if !time.Time(*s.EndsAt).After(time.Time(*s.StartsAt)) {
		return invalidSilenceField("endsAt", "invalid_time_range", "silence endsAt must be after startsAt")
	}
	if s.CreatedBy == nil {
		return invalidSilenceField("createdBy", "missing_required_field", "silence createdBy is required")
	}
	if s.Comment == nil {
		return invalidSilenceField("comment", "missing_required_field", "silence comment is required")
	}
	if s.Matchers == nil {
		return invalidSilenceField("matchers", "missing_required_field", "silence matchers are required")
	}
	if len(*s.Matchers) == 0 {
		return invalidSilenceField("matchers", "empty_matcher_set", "silence must contain at least one matcher")
	}
	allMatchEmpty := true
	for index, m := range *s.Matchers {
		prefix := fmt.Sprintf("matchers[%d]", index)
		if m.Name == nil {
			return invalidSilenceField(prefix+".name", "missing_required_field", "matcher name is required")
		}
		if strings.TrimSpace(*m.Name) == "" {
			return invalidSilenceField(prefix+".name", "empty_required_field", "matcher name cannot be empty")
		}
		if !silenceMatcherNamePattern.MatchString(*m.Name) {
			return invalidSilenceField(prefix+".name", "invalid_matcher_name", "matcher name is invalid")
		}
		if m.Value == nil {
			return invalidSilenceField(prefix+".value", "missing_required_field", "matcher value is required")
		}
		if m.IsRegex == nil {
			return invalidSilenceField(prefix+".isRegex", "missing_required_field", "matcher isRegex is required")
		}
		isEqual := m.IsEqual == nil || *m.IsEqual
		matchesEmpty := false
		if *m.IsRegex {
			compiled, err := internalRegex.CompileAnchored(*m.Value)
			if err != nil {
				return invalidSilenceField(prefix+".value", "invalid_regex", "matcher regular expression is invalid")
			}
			matchesEmpty = compiled.MatchString("") == isEqual
		} else {
			matchesEmpty = (*m.Value == "") == isEqual
		}
		allMatchEmpty = allMatchEmpty && matchesEmpty
	}
	if allMatchEmpty {
		return invalidSilenceField("matchers", "matches_empty", "at least one matcher must not match the empty string")
	}
	return nil
}

func invalidSilenceField(field, reasonCode, message string) *silenceValidationError {
	return &silenceValidationError{field: field, reasonCode: reasonCode, message: message}
}

func silenceDecodeError(err error) (string, string) {
	var semanticErr *json.SemanticError
	if errors.As(err, &semanticErr) {
		field := jsonPointerToSilenceField(string(semanticErr.JSONPointer))
		if isSilenceDateField(field) {
			return field, "invalid_format"
		}
		return field, "wrong_type"
	}
	var syntacticErr *jsontext.SyntacticError
	if errors.As(err, &syntacticErr) {
		return jsonPointerToSilenceField(string(syntacticErr.JSONPointer)), "wrong_type"
	}
	return "$", "invalid_entry"
}

func jsonPointerToSilenceField(pointer string) string {
	if pointer == "" {
		return "$"
	}
	parts := strings.Split(strings.TrimPrefix(pointer, "/"), "/")
	if len(parts) == 0 {
		return "$"
	}
	var field strings.Builder
	field.WriteString(strings.ReplaceAll(strings.ReplaceAll(parts[0], "~1", "/"), "~0", "~"))
	for _, raw := range parts[1:] {
		part := strings.ReplaceAll(strings.ReplaceAll(raw, "~1", "/"), "~0", "~")
		if _, err := strconv.Atoi(part); err == nil {
			field.WriteByte('[')
			field.WriteString(part)
			field.WriteByte(']')
		} else {
			field.WriteByte('.')
			field.WriteString(part)
		}
	}
	return field.String()
}

func isSilenceDateField(field string) bool {
	return field == "startsAt" || field == "endsAt" || field == "createdAt" || field == "updatedAt"
}

func extractSilenceString(raw jsontext.Value, field string) string {
	var object map[string]jsontext.Value
	if err := json.Unmarshal(raw, &object); err != nil {
		return ""
	}
	value, found := object[field]
	if !found || value.Kind() != '"' {
		return ""
	}
	var result string
	if err := json.Unmarshal(value, &result); err != nil {
		return ""
	}
	return result
}

func extractSilenceStatus(raw jsontext.Value) string {
	var object map[string]jsontext.Value
	if err := json.Unmarshal(raw, &object); err != nil {
		return "missing"
	}
	status, found := object["status"]
	if !found || status.Kind() != '{' {
		return "missing"
	}
	var statusObject map[string]jsontext.Value
	if err := json.Unmarshal(status, &statusObject); err != nil {
		return "missing"
	}
	state, found := statusObject["state"]
	if !found || state.Kind() != '"' {
		return "missing"
	}
	var result string
	if err := json.Unmarshal(state, &result); err != nil {
		return "missing"
	}
	return result
}

func silenceStatusValue(s silence) string {
	if s.Status == nil || s.Status.State == "" {
		return "missing"
	}
	return s.Status.State
}

func stringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func boolValue(value *bool) bool {
	return value != nil && *value
}
