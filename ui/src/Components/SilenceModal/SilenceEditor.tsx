import {
  CSSProperties,
  FC,
  ReactNode,
  useEffect,
  useId,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
} from "react";

import { observer } from "mobx-react-lite";

import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { faSpinner } from "@fortawesome/free-solid-svg-icons/faSpinner";

import { CommonOptions } from "Common/Fetch";
import { QueryOperators } from "Common/Query";
import { BackgroundClassMap } from "Common/Colors";
import {
  AlertmanagerSilencePayloadT,
  SilenceEditorLabelNamesResponseT,
  SilenceEditorErrorResponseT,
  SilenceEditorSubmitRequestT,
  SilenceEditorSubmitResponseT,
  SilenceEditorLabelValuesResponseT,
  SilenceEditorPreviewAlertT,
  SilenceEditorPreviewRequestT,
  SilenceEditorPreviewResponseT,
} from "Models/APITypes";
import { AlertStore, FormatBackendURI } from "Stores/AlertStore";
import {
  AlertmanagerClustersToOption,
  MatcherToExpression,
  SilenceFormStore,
} from "Stores/SilenceFormStore";
import { Settings } from "Stores/Settings";
import type { SilenceEditorCloseReasonT } from "./index";

const matcherPattern =
  /^\s*([a-zA-Z_:][a-zA-Z0-9_:]*)\s*(=~|!~|!=|=)\s*([\s\S]*?)\s*$/;
const matcherNamePrefixPattern = /^[a-zA-Z_:][a-zA-Z0-9_:]*$/;
const directSubmitTimeoutMs = 30_000;

const durationPattern = /([1-9][0-9]*)(ms|mo|y|w|d|h|m|s)/g;

type ParsedMatcherT = {
  name: string;
  value: string;
  isRegex: boolean;
  isEqual: boolean;
};

type DurationKindT = "preset" | "custom" | "absolute";

type DurationUnitT = "ms" | "s" | "m" | "h" | "d" | "w" | "mo" | "y";

type DurationPartT = {
  unit: DurationUnitT;
  value: number;
};

type DurationParseResultT = {
  parts: DurationPartT[];
  error: string | null;
};

type SuggestionStateT = {
  kind: "labels" | "values";
  index: number;
  items: Array<{ text: string; meta: string }>;
  loading: boolean;
  summary?: string;
};

type EditorInitialStateT = {
  target: string;
  expressions: string[];
  author: string;
  comment: string;
  startMode: "now" | "date";
  startDate: string;
  startTime: string;
  durationKind: DurationKindT;
  presetDuration: string;
  customDuration: string;
  endDate: string;
  endTime: string;
  initialStartsAtISO: string;
  initialEndsAtISO: string;
  initialEndDate: string;
  initialEndTime: string;
};

const PRESET_DURATIONS: Array<{ label: string; value: string }> = [
  { label: "In 1 hour", value: "1h" },
  { label: "In 2 hours", value: "2h" },
  { label: "In 6 hours", value: "6h" },
  { label: "In 12 hours", value: "12h" },
  { label: "In 1 day", value: "1d" },
  { label: "In 2 days", value: "2d" },
  { label: "In 4 days", value: "4d" },
  { label: "In 1 week", value: "1w" },
  { label: "In 1 month", value: "1mo" },
  { label: "In 1 year", value: "1y" },
  { label: "In 100 years", value: "100y" },
];

const SILENCE_EDITOR_AUTHOR_STORAGE_KEY = "karmaSilenceEditorAuthorV1";
const SILENCE_EDITOR_CONSOLE_MESSAGE = "Karma silence editor";

interface BrowserSubmitAuditBaseT {
  component: "silence_editor";
  correlation_id: string;
  operation: "create" | "edit" | "recreate";
  target: string;
  member: string;
  mode: "browser_routed";
}

function browserSubmitAuditBase(
  correlationID: string,
  operation: "create" | "edit" | "recreate",
  target: string,
  member: string,
): BrowserSubmitAuditBaseT {
  return {
    component: "silence_editor",
    correlation_id: correlationID,
    operation,
    target,
    member,
    mode: "browser_routed",
  };
}

function browserSubmitDuration(startedAt: number): number {
  return Math.max(0, Math.round(performance.now() - startedAt));
}

function readStoredAuthor(fallback: string): string {
  try {
    return localStorage.getItem(SILENCE_EDITOR_AUTHOR_STORAGE_KEY) ?? fallback;
  } catch (error) {
    console.warn(
      "Unable to read the saved silence author from localStorage.",
      error,
    );
    return fallback;
  }
}

function saveStoredAuthor(author: string): void {
  try {
    localStorage.setItem(SILENCE_EDITOR_AUTHOR_STORAGE_KEY, author);
  } catch (error) {
    console.warn("Unable to save the silence author to localStorage.", error);
  }
}

function escapeQuoted(value: string): string {
  return value
    .replaceAll("\\", "\\\\")
    .replaceAll("\n", "\\n")
    .replaceAll('"', '\\"');
}

function mapTargetToMembers(
  target: string,
  alertStore: AlertStore,
): { [cluster: string]: string[] } {
  let members = alertStore.data.clustersWithoutReadOnly[target];
  if (!members || members.length === 0) {
    members = alertStore.data.upstreams.instances
      .filter(
        (instance) => instance.cluster === target || instance.name === target,
      )
      .filter((instance) => !instance.readonly)
      .map((instance) => instance.name);
  }
  if (!members || members.length === 0) {
    return {};
  }
  return { [target]: members };
}

function unescapeQuoted(value: string): string {
  let out = "";
  for (let i = 0; i < value.length; i++) {
    const c = value[i];
    if (c !== "\\") {
      out += c;
      continue;
    }
    if (i + 1 >= value.length) {
      out += "\\";
      continue;
    }
    const n = value[i + 1];
    if (n === "\\" || n === '"') {
      out += n;
    } else if (n === "n") {
      out += "\n";
    } else {
      out += `\\${n}`;
    }
    i += 1;
  }
  return out;
}

function parseMatcherValue(rawValue: string): string | null {
  let value = rawValue;
  let expectTrailingQuote = false;
  if (value.startsWith('"')) {
    value = value.slice(1);
    expectTrailingQuote = true;
  }

  let escaped = false;
  for (let index = 0; index < value.length; index++) {
    const char = value[index];
    if (escaped) {
      escaped = false;
      continue;
    }
    if (char === "\\") {
      escaped = true;
      continue;
    }
    if (char === '"') {
      if (!expectTrailingQuote || index < value.length - 1) {
        return null;
      }
      expectTrailingQuote = false;
    }
  }
  if (expectTrailingQuote) {
    return null;
  }
  const valueWithoutTrailingQuote = rawValue.startsWith('"')
    ? value.slice(0, -1)
    : value;
  return unescapeQuoted(valueWithoutTrailingQuote);
}

export function parseMatcherExpression(value: string): ParsedMatcherT | null {
  const m = value.trim().match(matcherPattern);
  if (!m) {
    return null;
  }
  const name = m[1];
  const op = m[2];
  const rawValue = parseMatcherValue(m[3]);
  if (rawValue === null) {
    return null;
  }
  const isRegex = op === "=~" || op === "!~";
  return {
    name,
    value: rawValue,
    isRegex,
    isEqual: op === "=" || op === "=~",
  };
}

function matcherDomainFingerprint(expressions: string[]): string {
  const normalized: string[] = [];
  for (const expression of expressions) {
    const matcher = parseMatcherExpression(expression);
    if (matcher === null) {
      return JSON.stringify({ invalid: expressions });
    }
    normalized.push(
      JSON.stringify({
        name: matcher.name,
        value: matcher.value,
        isRegex: matcher.isRegex,
        isEqual: matcher.isEqual,
      }),
    );
  }
  normalized.sort();
  return JSON.stringify(normalized);
}

function renderRegexpSyntax(value: string): ReactNode[] {
  const result: ReactNode[] = [];
  let literal = "";
  let inCharacterClass = false;
  let key = 0;
  const flushLiteral = () => {
    if (literal !== "") {
      result.push(
        <span className="syntax-regexp-literal" key={`literal-${key++}`}>
          {literal}
        </span>,
      );
      literal = "";
    }
  };
  const appendMeta = (text: string) => {
    flushLiteral();
    result.push(
      <span className="syntax-regexp-meta" key={`meta-${key++}`}>
        {text}
      </span>,
    );
  };

  for (let index = 0; index < value.length;) {
    const character = value[index];
    if (character === "\\") {
      let end = Math.min(index + 2, value.length);
      const escaped = value[index + 1];
      if (
        (escaped === "p" || escaped === "P" || escaped === "x") &&
        value[end] === "{"
      ) {
        const closingBrace = value.indexOf("}", end + 1);
        if (closingBrace !== -1) {
          end = closingBrace + 1;
        }
      } else if (escaped && /[0-9]/.test(escaped)) {
        while (
          end < value.length &&
          end < index + 4 &&
          /[0-9]/.test(value[end])
        ) {
          end++;
        }
      }
      appendMeta(value.slice(index, end));
      index = end;
      continue;
    }
    if (character === "{") {
      const closingBrace = value.indexOf("}", index + 1);
      const quantifier =
        closingBrace === -1 ? "" : value.slice(index + 1, closingBrace);
      if (closingBrace !== -1 && /^\d+(,\d*)?$/.test(quantifier)) {
        appendMeta(value.slice(index, closingBrace + 1));
        index = closingBrace + 1;
        continue;
      }
    }
    if (character === "[") {
      appendMeta(character);
      inCharacterClass = true;
    } else if (character === "]") {
      appendMeta(character);
      inCharacterClass = false;
    } else if (".^$*+?()|{}".includes(character)) {
      appendMeta(character);
    } else if (inCharacterClass && (character === "-" || character === ":")) {
      appendMeta(character);
    } else {
      literal += character;
    }
    index++;
  }
  flushLiteral();
  return result;
}

const MatcherSyntax: FC<{ expression: string }> = ({ expression }) => {
  const parsed = parseMatcherExpression(expression);
  const parts = expression.match(
    /^(\s*)([a-zA-Z_:][a-zA-Z0-9_:]*)(\s*)(=~|!~|!=|=)(\s*)(?:(["])((?:\\.|[^"])*)((?:["]))|([\s\S]*?))(\s*)$/,
  );
  if (!parsed || !parts) {
    return <>{expression}</>;
  }
  const operator = parts[4];
  const isRegexp = operator.includes("~");
  const isNegative = operator.startsWith("!");
  const quoted = parts[6] === '"';
  const displayValue = (quoted ? parts[7] : parts[9]) || "";
  return (
    <>
      {parts[1]}
      <span className="syntax-label">{parts[2]}</span>
      {parts[3]}
      <span
        className={`syntax-operator${isRegexp ? " regexp" : ""}${
          isNegative ? " negative" : ""
        }`}
      >
        {operator}
      </span>
      {parts[5]}
      {quoted ? <span className="syntax-quote">{parts[6]}</span> : null}
      {isRegexp ? (
        <span className="syntax-value regexp">
          {renderRegexpSyntax(displayValue)}
        </span>
      ) : (
        <span className="syntax-value">{displayValue}</span>
      )}
      {quoted ? <span className="syntax-quote">{parts[8]}</span> : null}
      {parts[10]}
    </>
  );
};

function toDateInputValue(d: Date): string {
  const year = d.getFullYear();
  const month = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function toTimeInputValue(d: Date): string {
  const hours = String(d.getHours()).padStart(2, "0");
  const minutes = String(d.getMinutes()).padStart(2, "0");
  return `${hours}:${minutes}`;
}

export function mergeDateTime(
  dateValue: string,
  timeValue: string,
): Date | null {
  const dateMatch = dateValue.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  const timeMatch = timeValue.match(/^(\d{2}):(\d{2})$/);
  if (!dateMatch || !timeMatch) {
    return null;
  }
  const year = Number(dateMatch[1]);
  const month = Number(dateMatch[2]) - 1;
  const day = Number(dateMatch[3]);
  const hours = Number(timeMatch[1]);
  const minutes = Number(timeMatch[2]);
  const d = new Date(year, month, day, hours, minutes, 0, 0);
  if (
    Number.isNaN(d.getTime()) ||
    d.getFullYear() !== year ||
    d.getMonth() !== month ||
    d.getDate() !== day ||
    d.getHours() !== hours ||
    d.getMinutes() !== minutes
  ) {
    return null;
  }
  return d;
}

function parseDuration(input: string): DurationParseResultT {
  if (input === "") {
    return { parts: [], error: "Duration is required" };
  }
  if (input !== input.trim()) {
    return { parts: [], error: "Use canonical format, e.g. 1w2d6h or 1y" };
  }
  const text = input;
  const parts: DurationPartT[] = [];
  durationPattern.lastIndex = 0;
  let consumed = "";
  let m = durationPattern.exec(text);
  while (m) {
    const value = Number(m[1]);
    if (!Number.isSafeInteger(value)) {
      return { parts: [], error: "Duration value is too large" };
    }
    const unit = m[2] as DurationUnitT;
    parts.push({ value, unit });
    consumed += m[0];
    m = durationPattern.exec(text);
  }
  if (consumed !== text) {
    return { parts: [], error: "Use canonical format, e.g. 1w2d6h or 1y" };
  }

  const rank: Record<DurationUnitT, number> = {
    y: 0,
    mo: 1,
    w: 2,
    d: 3,
    h: 4,
    m: 5,
    s: 6,
    ms: 7,
  };

  const hasCalendar = parts.some((p) => p.unit === "mo" || p.unit === "y");
  if (hasCalendar && parts.length > 1) {
    return { parts: [], error: "Calendar units mo/y must be standalone" };
  }

  for (let i = 1; i < parts.length; i++) {
    if (rank[parts[i].unit] <= rank[parts[i - 1].unit]) {
      return {
        parts: [],
        error: "Units must be unique and in descending order",
      };
    }
  }

  return { parts, error: null };
}

function addMonthsClamped(base: Date, months: number): Date {
  const d = new Date(base.getTime());
  const day = d.getDate();
  d.setDate(1);
  d.setMonth(d.getMonth() + months);
  const lastDay = new Date(d.getFullYear(), d.getMonth() + 1, 0).getDate();
  d.setDate(Math.min(day, lastDay));
  return d;
}

function addYearsClamped(base: Date, years: number): Date {
  return addMonthsClamped(base, years * 12);
}

function applyDuration(base: Date, parts: DurationPartT[]): Date {
  const fixedUnitMs: Record<Exclude<DurationUnitT, "mo" | "y">, number> = {
    w: 7 * 24 * 60 * 60 * 1000,
    d: 24 * 60 * 60 * 1000,
    h: 60 * 60 * 1000,
    m: 60 * 1000,
    s: 1000,
    ms: 1,
  };
  const out = new Date(base.getTime());
  for (const part of parts) {
    switch (part.unit) {
      case "y":
        return addYearsClamped(out, part.value);
      case "mo":
        return addMonthsClamped(out, part.value);
      case "w":
      case "d":
      case "h":
      case "m":
      case "s":
      case "ms":
        out.setTime(out.getTime() + part.value * fixedUnitMs[part.unit]);
        break;
    }
  }
  return out;
}

function formatLocalTimestamp(d: Date): string {
  return d.toLocaleString([], {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    timeZoneName: "short",
  });
}

function formatUTCTimestamp(d: Date): string {
  return d.toLocaleString([], {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    timeZone: "UTC",
    timeZoneName: "short",
  });
}

function sameMembers(left: string[], right: string[]): boolean {
  if (left.length !== right.length) {
    return false;
  }
  const sortedLeft = [...left].sort();
  const sortedRight = [...right].sort();
  return sortedLeft.every((member, index) => member === sortedRight[index]);
}

function resolveStoredTarget(
  alertmanagers: Array<{ value: string[] }>,
  clusters: { [cluster: string]: string[] },
): string {
  for (const selected of alertmanagers) {
    const match = Object.entries(clusters).find(([, members]) =>
      sameMembers(selected.value, members),
    );
    if (match) {
      return match[0];
    }
  }
  return "";
}

function formatFixedDuration(durationMs: number): string {
  if (!Number.isFinite(durationMs) || durationMs <= 0) {
    return "1h";
  }
  const units: Array<[string, number]> = [
    ["w", 7 * 24 * 60 * 60 * 1000],
    ["d", 24 * 60 * 60 * 1000],
    ["h", 60 * 60 * 1000],
    ["m", 60 * 1000],
    ["s", 1000],
    ["ms", 1],
  ];
  let remaining = Math.floor(durationMs);
  let result = "";
  for (const [unit, unitMs] of units) {
    const value = Math.floor(remaining / unitMs);
    if (value > 0) {
      result += `${value}${unit}`;
      remaining -= value * unitMs;
    }
  }
  return result || "1h";
}

function matcherExpressionsFromFilters(alertStore: AlertStore): string[] {
  const allowedMatchers = new Set<string>([
    QueryOperators.Equal,
    QueryOperators.NotEqual,
    QueryOperators.Regex,
    QueryOperators.NegativeRegex,
  ]);
  return alertStore.filters.values
    .filter(
      (filter) =>
        !filter.name.startsWith("@") &&
        allowedMatchers.has(filter.matcher) &&
        !alertStore.settings.values.silenceForm.strip.labels.includes(
          filter.name,
        ),
    )
    .map((filter) => {
      const isRegex =
        filter.matcher === QueryOperators.Regex ||
        filter.matcher === QueryOperators.NegativeRegex;
      const value = isRegex ? `(?i:.*(?:${filter.value}).*)` : filter.value;
      return `${filter.name}${filter.matcher}"${escapeQuoted(value)}"`;
    });
}

function buildInitialState(
  alertStore: AlertStore,
  silenceFormStore: SilenceFormStore,
  settingsStore: Settings,
): EditorInitialStateT {
  const clusters = alertStore.data.clustersWithoutReadOnly;
  const operation = silenceFormStore.data.operation;
  const storedTarget = resolveStoredTarget(
    silenceFormStore.data.alertmanagers,
    clusters,
  );
  const configuredTarget = Object.entries(clusters).find(([, members]) =>
    members.some((member) =>
      alertStore.settings.values.silenceForm.defaultAlertmanagers.includes(
        member,
      ),
    ),
  )?.[0];
  const target =
    (operation !== "create" || !silenceFormStore.data.autofillMatchers
      ? storedTarget
      : "") ||
    configuredTarget ||
    Object.keys(clusters)[0] ||
    "";

  let expressions: string[];
  if (operation === "create" && silenceFormStore.data.autofillMatchers) {
    expressions = matcherExpressionsFromFilters(alertStore);
  } else {
    expressions = silenceFormStore.data.matchers.map(MatcherToExpression);
  }
  if (expressions.length === 0) {
    expressions = [""];
  }

  const startsAt = silenceFormStore.data.startsAt;
  const endsAt = silenceFormStore.data.endsAt;
  const isEdit = operation === "edit";
  const isRecreate = operation === "recreate";
  const author =
    operation !== "create"
      ? silenceFormStore.data.author
      : alertStore.info.authentication.enabled
        ? alertStore.info.authentication.username
        : readStoredAuthor(settingsStore.silenceFormConfig.config.author || "");

  return {
    target,
    expressions,
    author,
    comment: operation === "create" ? "" : silenceFormStore.data.comment,
    startMode: isEdit ? "date" : "now",
    startDate: toDateInputValue(startsAt),
    startTime: toTimeInputValue(startsAt),
    durationKind: isEdit ? "absolute" : isRecreate ? "custom" : "preset",
    presetDuration: "1h",
    customDuration: isRecreate
      ? formatFixedDuration(endsAt.getTime() - startsAt.getTime())
      : "1h",
    endDate: toDateInputValue(endsAt),
    endTime: toTimeInputValue(endsAt),
    initialStartsAtISO: startsAt.toISOString(),
    initialEndsAtISO: endsAt.toISOString(),
    initialEndDate: toDateInputValue(endsAt),
    initialEndTime: toTimeInputValue(endsAt),
  };
}

const SilenceEditor: FC<{
  alertStore: AlertStore;
  silenceFormStore: SilenceFormStore;
  settingsStore: Settings;
  onClose: () => void;
  onSubmitted: (target: string, silenceID: string) => void;
  registerCloseGuard: (
    guard: (reason: SilenceEditorCloseReasonT) => boolean,
  ) => void;
}> = ({
  alertStore,
  silenceFormStore,
  settingsStore,
  onClose,
  onSubmitted,
  registerCloseGuard,
}) => {
  const initial = useRef(
    buildInitialState(alertStore, silenceFormStore, settingsStore),
  ).current;
  const matcherComboboxId = useId();
  const editorFieldId = useId();
  const defaultTargets = useMemo(
    () => Object.keys(alertStore.data.clustersWithoutReadOnly),
    [alertStore.data.clustersWithoutReadOnly],
  );

  const [target, setTarget] = useState<string>(initial.target);
  const [expressions, setExpressions] = useState<string[]>(initial.expressions);
  const [author, setAuthor] = useState<string>(initial.author);
  const [comment, setComment] = useState<string>(initial.comment);
  const [startMode, setStartMode] = useState<"now" | "date">(initial.startMode);
  const [startDate, setStartDate] = useState<string>(initial.startDate);
  const [startTime, setStartTime] = useState<string>(initial.startTime);
  const [durationKind, setDurationKind] = useState<DurationKindT>(
    initial.durationKind,
  );
  const [presetDuration, setPresetDuration] = useState<string>(
    initial.presetDuration,
  );
  const [customDuration, setCustomDuration] = useState<string>(
    initial.customDuration,
  );
  const [endDate, setEndDate] = useState<string>(initial.endDate);
  const [endTime, setEndTime] = useState<string>(initial.endTime);

  const [preview, setPreview] = useState<SilenceEditorPreviewAlertT[]>([]);
  const [previewTotal, setPreviewTotal] = useState<number>(0);
  const [previewNextCursor, setPreviewNextCursor] = useState<string | null>(
    null,
  );
  const [previewSnapshot, setPreviewSnapshot] = useState<string | null>(null);
  const [previewRefreshNonce, setPreviewRefreshNonce] = useState(0);
  const [previewExcludedNoRawLabels, setPreviewExcludedNoRawLabels] =
    useState<number>(0);
  const [estimateNow, setEstimateNow] = useState<Date>(() => new Date());
  const [previewRefreshing, setPreviewRefreshing] = useState<boolean>(false);
  const [previewStale, setPreviewStale] = useState<boolean>(false);
  const [previewError, setPreviewError] = useState<string | null>(null);
  const [matcherPreflightValid, setMatcherPreflightValid] =
    useState<boolean>(true);

  const [submitting, setSubmitting] = useState<boolean>(false);
  const [submitError, setSubmitError] = useState<string | null>(null);
  const [submitSuccess, setSubmitSuccess] = useState<string | null>(null);
  const [submitOutcomeUnknown, setSubmitOutcomeUnknown] =
    useState<boolean>(false);
  const [submitRequiresReopen, setSubmitRequiresReopen] =
    useState<boolean>(false);

  const [editingMatcher, setEditingMatcher] = useState<number | null>(null);
  const [suggestions, setSuggestions] = useState<SuggestionStateT | null>(null);
  const [activeSuggestionIndex, setActiveSuggestionIndex] = useState<number>(0);
  const [suggestionMenuStyle, setSuggestionMenuStyle] = useState<CSSProperties>();

  const previewGenerationRef = useRef(0);
  const previewDebounceRef = useRef<number | null>(null);
  const closeTimerRef = useRef<number | null>(null);
  const initialAuthorRef = useRef<string>(initial.author);
  const matcherEditBackupRef = useRef<{ [index: number]: string }>({});
  const matcherInputsRef = useRef<Array<HTMLInputElement | null>>([]);
  const suggestionListsRef = useRef<Array<HTMLDivElement | null>>([]);
  const suggestionPointerDownRef = useRef(false);
  const suggestionGenerationRef = useRef(0);
  const suggestionDebounceRef = useRef<number | null>(null);
  const suggestionAbortRef = useRef<AbortController | null>(null);
  const proxySubmitAbortRef = useRef<AbortController | null>(null);
  const directSubmitAbortRef = useRef<AbortController | null>(null);
  const submitGenerationRef = useRef(0);
  const mountedRef = useRef(true);

  useEffect(() => {
    if (silenceFormStore.data.editorSessionState === "closed") {
      silenceFormStore.data.transitionEditorSession("BEGIN_CREATE");
    }
  }, [silenceFormStore]);
  const matcherPreflightInitializedRef = useRef(false);

  useLayoutEffect(() => {
    const updateSuggestionMenuPosition = () => {
      if (editingMatcher === null || suggestions?.index !== editingMatcher) {
        setSuggestionMenuStyle(undefined);
        return;
      }

      const input = matcherInputsRef.current[editingMatcher];
      const modal = input?.closest(".modal-content");
      if (!input || !modal) {
        return;
      }

      const inputRect = input.getBoundingClientRect();
      const modalRect = modal.getBoundingClientRect();
      const edgePadding = 8;
      const width = Math.min(448, modalRect.width - edgePadding * 2);
      const left = Math.max(
        modalRect.left + edgePadding,
        Math.min(inputRect.left, modalRect.right - edgePadding - width),
      );
      setSuggestionMenuStyle({
        width: `${width}px`,
        left: `${left - inputRect.left}px`,
      });
    };

    updateSuggestionMenuPosition();
    window.addEventListener("resize", updateSuggestionMenuPosition);
    return () => window.removeEventListener("resize", updateSuggestionMenuPosition);
  }, [editingMatcher, suggestions]);

  useLayoutEffect(() => {
    const list =
      editingMatcher === null ? null : suggestionListsRef.current[editingMatcher];
    const activeOption = list?.querySelector<HTMLElement>(
      '[aria-selected="true"]',
    );
    if (!list || !activeOption) {
      return;
    }
    const listRect = list.getBoundingClientRect();
    const optionRect = activeOption.getBoundingClientRect();
    if (optionRect.top < listRect.top) {
      list.scrollTop += optionRect.top - listRect.top;
    } else if (optionRect.bottom > listRect.bottom) {
      list.scrollTop += optionRect.bottom - listRect.bottom;
    }
  }, [activeSuggestionIndex, suggestions]);

  useEffect(() => {
    if (editingMatcher === null || suggestions === null) {
      return;
    }

    const closeSuggestionsOutside = (event: PointerEvent) => {
      const input = matcherInputsRef.current[editingMatcher];
      if (
        event.target instanceof Node &&
        !input?.contains(event.target) &&
        !suggestionListsRef.current[editingMatcher]?.contains(event.target)
      ) {
        setSuggestions(null);
      }
    };
    window.addEventListener("pointerdown", closeSuggestionsOutside);
    return () => window.removeEventListener("pointerdown", closeSuggestionsOutside);
  }, [editingMatcher, suggestions]);

  useEffect(() => {
    if (!target && defaultTargets.length > 0) {
      setTarget(defaultTargets[0]);
    }
  }, [defaultTargets, target]);

  useEffect(() => {
    suggestionGenerationRef.current++;
    if (suggestionDebounceRef.current !== null) {
      window.clearTimeout(suggestionDebounceRef.current);
      suggestionDebounceRef.current = null;
    }
    if (suggestionAbortRef.current !== null) {
      suggestionAbortRef.current.abort();
      suggestionAbortRef.current = null;
    }
    setSuggestions(null);
  }, [target]);

  useEffect(() => {
    silenceFormStore.data.setAutofillMatchers(false);
  }, [silenceFormStore]);

  useEffect(() => {
    if (editingMatcher !== null) {
      matcherInputsRef.current[editingMatcher]?.focus();
    }
  }, [editingMatcher]);

  useEffect(() => {
    mountedRef.current = true;
    return () => {
      mountedRef.current = false;
      submitGenerationRef.current += 1;
      if (previewDebounceRef.current !== null) {
        window.clearTimeout(previewDebounceRef.current);
      }
      if (closeTimerRef.current !== null) {
        window.clearTimeout(closeTimerRef.current);
      }
      if (suggestionDebounceRef.current !== null) {
        window.clearTimeout(suggestionDebounceRef.current);
      }
      if (suggestionAbortRef.current !== null) {
        suggestionAbortRef.current.abort();
      }
      if (directSubmitAbortRef.current !== null) {
        directSubmitAbortRef.current.abort();
      }
      if (proxySubmitAbortRef.current !== null) {
        proxySubmitAbortRef.current.abort();
      }
    };
  }, []);

  useEffect(() => {
    if (durationKind === "absolute") {
      return;
    }
    const timer = window.setInterval(() => {
      setEstimateNow(new Date());
    }, 1000);
    return () => {
      window.clearInterval(timer);
    };
  }, [durationKind]);

  const parsedMatchers = useMemo(
    () => expressions.map((e) => parseMatcherExpression(e)),
    [expressions],
  );
  const allMatchersValid = parsedMatchers.every((m) => m !== null);
  const compiledMatchers = parsedMatchers.filter(
    (m): m is ParsedMatcherT => m !== null,
  );

  const endChanged = useMemo(() => {
    if (durationKind !== "absolute") {
      return true;
    }
    return (
      endDate !== initial.initialEndDate || endTime !== initial.initialEndTime
    );
  }, [
    durationKind,
    endDate,
    endTime,
    initial.initialEndDate,
    initial.initialEndTime,
  ]);

  const startAtBase = useMemo(() => {
    if (startMode === "now") {
      return estimateNow;
    }
    return mergeDateTime(startDate, startTime);
  }, [estimateNow, startDate, startMode, startTime]);

  const durationParse = useMemo(
    () =>
      durationKind === "custom"
        ? parseDuration(customDuration)
        : parseDuration(presetDuration),
    [customDuration, durationKind, presetDuration],
  );

  const endsAt = useMemo(() => {
    if (!startAtBase) {
      return null;
    }
    if (durationKind === "absolute") {
      return mergeDateTime(endDate, endTime);
    }
    if (durationParse.error) {
      return null;
    }
    const estimateTime = estimateNow;
    const anchor =
      startAtBase.getTime() > estimateTime.getTime()
        ? startAtBase
        : estimateTime;
    const result = applyDuration(anchor, durationParse.parts);
    return Number.isFinite(result.getTime()) ? result : null;
  }, [
    durationKind,
    durationParse.error,
    durationParse.parts,
    estimateNow,
    endDate,
    endTime,
    startAtBase,
  ]);

  const matcherError = useMemo(() => {
    if (expressions.length === 0) {
      return "Add at least one matcher";
    }
    if (!allMatchersValid) {
      return 'Each matcher must use full expression format: label="value"';
    }
    return null;
  }, [allMatchersValid, expressions.length]);

  const timeError = useMemo(() => {
    if (!startAtBase) {
      return "Invalid start time";
    }
    if (!endsAt) {
      if (durationKind === "absolute") {
        return "Invalid end time";
      }
      return durationParse.error || "Invalid duration";
    }
    if (endsAt.getTime() <= startAtBase.getTime()) {
      return "End time must be after start time";
    }
    if (
      durationKind === "absolute" &&
      endsAt.getTime() <= estimateNow.getTime() &&
      !(silenceFormStore.data.operation === "edit" && !endChanged)
    ) {
      return "End time must be in the future";
    }
    return null;
  }, [durationKind, durationParse.error, endsAt, estimateNow, startAtBase]);

  const formError = useMemo(() => {
    if (!target) {
      return "Target is required";
    }
    if (matcherError) {
      return matcherError;
    }
    if (!author.trim()) {
      return "Author is required";
    }
    if (!comment.trim()) {
      return "Comment is required";
    }
    if (timeError) {
      return timeError;
    }
    return null;
  }, [author, comment, matcherError, target, timeError]);

  const previewInputError = useMemo(() => {
    if (!target) {
      return "Target is required";
    }
    return matcherError;
  }, [matcherError, target]);

  const dirty = useMemo(() => {
    return (
      target !== initial.target ||
      matcherDomainFingerprint(expressions) !==
        matcherDomainFingerprint(initial.expressions) ||
      author !== initial.author ||
      comment !== initial.comment ||
      startMode !== initial.startMode ||
      (startMode === "date" &&
        (startDate !== initial.startDate || startTime !== initial.startTime)) ||
      durationKind !== initial.durationKind ||
      (durationKind === "preset" &&
        presetDuration !== initial.presetDuration) ||
      (durationKind === "custom" &&
        customDuration !== initial.customDuration) ||
      (durationKind === "absolute" &&
        (endDate !== initial.endDate || endTime !== initial.endTime))
    );
  }, [
    author,
    comment,
    customDuration,
    durationKind,
    endDate,
    endTime,
    expressions,
    initial,
    presetDuration,
    startDate,
    startMode,
    startTime,
    target,
  ]);

  const fetchSuggestions = async (
    index: number,
    expression: string,
    caret: number,
  ) => {
    const generation = suggestionGenerationRef.current + 1;
    suggestionGenerationRef.current = generation;
    if (!target) {
      setSuggestions(null);
      return;
    }
    const valueMode = expression.match(/^([a-zA-Z_:][a-zA-Z0-9_:]*)=""$/);
    const inEmptyQuotes =
      valueMode && caret === expression.length - 1 && expression.length >= 2;

    if (suggestionDebounceRef.current !== null) {
      window.clearTimeout(suggestionDebounceRef.current);
      suggestionDebounceRef.current = null;
    }
    if (suggestionAbortRef.current !== null) {
      suggestionAbortRef.current.abort();
      suggestionAbortRef.current = null;
    }

    if (inEmptyQuotes) {
      setSuggestions({ kind: "values", index, items: [], loading: true });
      suggestionDebounceRef.current = window.setTimeout(async () => {
        const controller = new AbortController();
        suggestionAbortRef.current = controller;
        try {
          const params = new URLSearchParams({ target, name: valueMode[1] });
          const res = await fetch(
            `${FormatBackendURI("silence-editor/values")}?${params.toString()}`,
            {
              ...CommonOptions,
              method: "GET",
              signal: controller.signal,
            },
          );
          if (!res.ok) {
            if (suggestionGenerationRef.current === generation) {
              setSuggestions(null);
            }
            return;
          }
          const payload =
            (await res.json()) as SilenceEditorLabelValuesResponseT;
          if (suggestionGenerationRef.current !== generation) {
            return;
          }
          setSuggestions({
            kind: "values",
            index,
            items: payload.values.map((v) => ({
              text: v.value,
              meta: `${v.hits} alerts`,
            })),
            loading: false,
            summary: payload.truncated
              ? `Showing ${payload.values.length} of ${payload.total} values`
              : undefined,
          });
        } catch (err) {
          if ((err as Error).name === "AbortError") {
            return;
          }
          if (suggestionGenerationRef.current === generation) {
            setSuggestions(null);
          }
        } finally {
          if (suggestionAbortRef.current === controller) {
            suggestionAbortRef.current = null;
          }
        }
      }, 220);
      return;
    }

    const labelPrefix = expression.trim();
    if (labelPrefix !== "" && !matcherNamePrefixPattern.test(labelPrefix)) {
      setSuggestions(null);
      return;
    }

    setSuggestions({ kind: "labels", index, items: [], loading: true });
    suggestionDebounceRef.current = window.setTimeout(async () => {
      const controller = new AbortController();
      suggestionAbortRef.current = controller;
      try {
        const params = new URLSearchParams({ target, term: labelPrefix });
        const res = await fetch(
          `${FormatBackendURI("silence-editor/labels")}?${params.toString()}`,
          {
            ...CommonOptions,
            method: "GET",
            signal: controller.signal,
          },
        );
        if (!res.ok) {
          if (suggestionGenerationRef.current === generation) {
            setSuggestions(null);
          }
          return;
        }
        const payload = (await res.json()) as SilenceEditorLabelNamesResponseT;
        if (suggestionGenerationRef.current !== generation) {
          return;
        }
        setSuggestions({
          kind: "labels",
          index,
          items: payload.labels.map((v) => ({
            text: v.name,
            meta: `${v.values} values`,
          })),
          loading: false,
          summary: payload.truncated
            ? `Showing ${payload.labels.length} of ${payload.total} label names`
            : undefined,
        });
      } catch (err) {
        if ((err as Error).name === "AbortError") {
          return;
        }
        if (suggestionGenerationRef.current === generation) {
          setSuggestions(null);
        }
      } finally {
        if (suggestionAbortRef.current === controller) {
          suggestionAbortRef.current = null;
        }
      }
    }, 220);
  };

  const applySuggestion = (
    index: number,
    suggestion: { text: string; meta: string },
  ) => {
    const suggestionKind = suggestions?.kind;
    const next = [...expressions];
    if (suggestionKind === "labels") {
      next[index] = `${suggestion.text}=""`;
    } else if (suggestionKind === "values") {
      const expression = expressions[index];
      const match = expression.match(/^([a-zA-Z_:][a-zA-Z0-9_:]*)=""$/);
      if (match) {
        next[index] = `${match[1]}="${escapeQuoted(suggestion.text)}"`;
      }
    }
    setExpressions(next);
    suggestionGenerationRef.current++;
    setSuggestions(null);
    setActiveSuggestionIndex(0);
    window.setTimeout(() => {
      const input = matcherInputsRef.current[index];
      if (!input) {
        return;
      }
      input.focus();
      if (suggestionKind === "labels") {
        const caret = next[index].length - 1;
        input.setSelectionRange(caret, caret);
        fetchSuggestions(index, next[index], caret);
      }
    });
  };

  const removeMatcherAt = (removeIndex: number) => {
    suggestionGenerationRef.current++;
    if (suggestionDebounceRef.current !== null) {
      window.clearTimeout(suggestionDebounceRef.current);
      suggestionDebounceRef.current = null;
    }
    if (suggestionAbortRef.current !== null) {
      suggestionAbortRef.current.abort();
      suggestionAbortRef.current = null;
    }
    setSuggestions(null);
    setActiveSuggestionIndex(0);

    setExpressions(expressions.filter((_, index) => index !== removeIndex));

    const remappedBackup: { [index: number]: string } = {};
    for (const [key, value] of Object.entries(matcherEditBackupRef.current)) {
      const index = Number(key);
      if (Number.isNaN(index) || index === removeIndex) {
        continue;
      }
      remappedBackup[index > removeIndex ? index - 1 : index] = value;
    }
    matcherEditBackupRef.current = remappedBackup;

    if (editingMatcher === null) {
      return;
    }
    if (editingMatcher === removeIndex) {
      setEditingMatcher(null);
      return;
    }
    if (editingMatcher > removeIndex) {
      setEditingMatcher(editingMatcher - 1);
    }
  };

  useEffect(() => {
    const generation = previewGenerationRef.current + 1;
    previewGenerationRef.current = generation;
    if (previewDebounceRef.current !== null) {
      window.clearTimeout(previewDebounceRef.current);
      previewDebounceRef.current = null;
    }

    setPreviewStale(preview.length > 0);
    setPreviewNextCursor(null);
    setPreviewSnapshot(null);
    setPreviewRefreshing(false);
    if (matcherPreflightInitializedRef.current) {
      setMatcherPreflightValid(false);
    } else {
      matcherPreflightInitializedRef.current = true;
    }

    if (previewInputError !== null) {
      setPreviewError(previewInputError);
      return;
    }

    previewDebounceRef.current = window.setTimeout(async () => {
      setPreviewRefreshing(true);
      setPreviewStale(preview.length > 0);
      setPreviewError(null);
      const req: SilenceEditorPreviewRequestT = {
        target,
        matchers: compiledMatchers,
        limit: 100,
      };
      try {
        const res = await fetch(FormatBackendURI("silence-editor/preview"), {
          ...CommonOptions,
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(req),
        });
        const payload = (await res.json()) as
          SilenceEditorPreviewResponseT | SilenceEditorErrorResponseT;
        if (previewGenerationRef.current !== generation) {
          return;
        }
        if (!res.ok) {
          setPreviewError(
            (payload as SilenceEditorErrorResponseT).error || "Preview failed",
          );
          setMatcherPreflightValid(false);
          setPreviewRefreshing(false);
          return;
        }
        const data = payload as SilenceEditorPreviewResponseT;
        if (!Array.isArray(data.alerts)) {
          setPreviewError("Preview failed");
          setMatcherPreflightValid(false);
          setPreviewRefreshing(false);
          return;
        }
        setPreview(data.alerts);
        setPreviewTotal(Number.isFinite(data.total) ? data.total : 0);
        setPreviewNextCursor(data.nextCursor || null);
        setPreviewSnapshot(data.snapshot || null);
        setPreviewExcludedNoRawLabels(
          Number.isFinite(data.excludedNoRawLabels)
            ? data.excludedNoRawLabels
            : 0,
        );
        setMatcherPreflightValid(true);
        setPreviewStale(false);
      } catch (err) {
        if (previewGenerationRef.current !== generation) {
          return;
        }
        setMatcherPreflightValid(false);
        setPreviewError(
          err instanceof Error ? err.message : `unknown error: ${err}`,
        );
      } finally {
        if (previewGenerationRef.current === generation) {
          setPreviewRefreshing(false);
        }
      }
    }, 350);
  }, [expressions, previewInputError, previewRefreshNonce, target]);

  const canClose = (_reason: SilenceEditorCloseReasonT) => {
    const sessionState = silenceFormStore.data.editorSessionState;
    if (sessionState === "success" || sessionState === "outcomeUnknown") {
      return true;
    }
    if (sessionState === "submitting") {
      return false;
    }
    if (dirty) {
      const confirmClose = window.confirm("Discard unsaved silence changes?");
      if (!confirmClose) {
        return false;
      }
    }
    return true;
  };

  useEffect(() => {
    registerCloseGuard(canClose);
    return () => registerCloseGuard(() => true);
  }, [dirty, registerCloseGuard, silenceFormStore.data.editorSessionState]);

  const loadMorePreview = async () => {
    if (
      !previewNextCursor ||
      previewInputError !== null ||
      previewRefreshing ||
      previewStale
    ) {
      return;
    }
    if (previewDebounceRef.current !== null) {
      window.clearTimeout(previewDebounceRef.current);
      previewDebounceRef.current = null;
    }
    const generation = previewGenerationRef.current + 1;
    previewGenerationRef.current = generation;
    setPreviewRefreshing(true);
    setPreviewError(null);
    const req: SilenceEditorPreviewRequestT = {
      target,
      matchers: compiledMatchers,
      cursor: previewNextCursor,
      snapshot: previewSnapshot || undefined,
      limit: 100,
    };
    try {
      const res = await fetch(FormatBackendURI("silence-editor/preview"), {
        ...CommonOptions,
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(req),
      });
      const payload = (await res.json()) as
        SilenceEditorPreviewResponseT | SilenceEditorErrorResponseT;
      if (previewGenerationRef.current !== generation) {
        return;
      }
      if (!res.ok) {
        if (res.status === 409) {
          setPreviewSnapshot(null);
          setPreviewNextCursor(null);
          setMatcherPreflightValid(false);
          setPreviewRefreshNonce((value) => value + 1);
        }
        setPreviewError(
          (payload as SilenceEditorErrorResponseT).error || "Preview failed",
        );
        return;
      }
      const data = payload as SilenceEditorPreviewResponseT;
      if (!Array.isArray(data.alerts)) {
        setPreviewError("Preview failed");
        return;
      }
      setPreview((current) => [...current, ...data.alerts]);
      setPreviewTotal(Number.isFinite(data.total) ? data.total : 0);
      setPreviewNextCursor(data.nextCursor || null);
      setPreviewSnapshot(data.snapshot || previewSnapshot);
      setPreviewExcludedNoRawLabels(
        Number.isFinite(data.excludedNoRawLabels)
          ? data.excludedNoRawLabels
          : 0,
      );
      setPreviewStale(false);
    } catch (err) {
      if (previewGenerationRef.current === generation) {
        setPreviewError(
          err instanceof Error ? err.message : `unknown error: ${err}`,
        );
      }
    } finally {
      if (previewGenerationRef.current === generation) {
        setPreviewRefreshing(false);
      }
    }
  };

  const submit = async () => {
    if (
      submitting ||
      submitSuccess !== null ||
      formError !== null ||
      !startAtBase ||
      submitOutcomeUnknown ||
      submitRequiresReopen ||
      silenceFormStore.data.editorSessionState !== "editing"
    ) {
      return;
    }

    const generation = submitGenerationRef.current + 1;
    submitGenerationRef.current = generation;
    const isActiveGeneration = () =>
      mountedRef.current && submitGenerationRef.current === generation;

    const safeSetSubmitting = (value: boolean) => {
      if (isActiveGeneration()) {
        setSubmitting(value);
      }
    };

    const safeSetSubmitError = (value: string | null) => {
      if (isActiveGeneration()) {
        setSubmitError(value);
        if (value !== null) {
          silenceFormStore.data.transitionEditorSession("SUBMIT_KNOWN_FAILURE");
        }
      }
    };

    const safeSetSubmitOutcomeUnknown = (value: boolean) => {
      if (isActiveGeneration()) {
        setSubmitOutcomeUnknown(value);
        if (value) {
          silenceFormStore.data.transitionEditorSession("SUBMIT_UNKNOWN");
        }
      }
    };

    const safeSetSubmitRequiresReopen = (value: boolean) => {
      if (isActiveGeneration()) {
        setSubmitRequiresReopen(value);
      }
    };

    const safeSetSubmitSuccess = (value: string) => {
      if (isActiveGeneration()) {
        setSubmitSuccess(value);
        silenceFormStore.data.transitionEditorSession("SUBMIT_SUCCESS");
      }
    };

    safeSetSubmitError(null);
    safeSetSubmitOutcomeUnknown(false);
    safeSetSubmitRequiresReopen(false);
    silenceFormStore.data.transitionEditorSession("BEGIN_SUBMIT");
    safeSetSubmitting(true);

    const submitTime = new Date();
    const startChanged =
      startMode !== initial.startMode ||
      (startMode === "date" &&
        (startDate !== initial.startDate || startTime !== initial.startTime));
    const startsAt =
      silenceFormStore.data.operation === "edit" &&
      !startChanged &&
      initial.startMode === "date"
        ? new Date(initial.initialStartsAtISO)
        : startMode === "now"
          ? submitTime
          : new Date(startAtBase.getTime());
    const submittedEndsAt =
      durationKind === "absolute"
        ? silenceFormStore.data.operation === "edit" &&
          !endChanged &&
          initial.durationKind === "absolute"
          ? new Date(initial.initialEndsAtISO)
          : mergeDateTime(endDate, endTime)
        : durationParse.error === null
          ? applyDuration(
              startsAt.getTime() > submitTime.getTime() ? startsAt : submitTime,
              durationParse.parts,
            )
          : null;
    const allowPastEndForUnchangedEdit =
      silenceFormStore.data.operation === "edit" &&
      durationKind === "absolute" &&
      !endChanged;

    if (
      submittedEndsAt === null ||
      !Number.isFinite(submittedEndsAt.getTime()) ||
      submittedEndsAt.getTime() <= startsAt.getTime() ||
      (!allowPastEndForUnchangedEdit &&
        submittedEndsAt.getTime() <= submitTime.getTime())
    ) {
      safeSetSubmitError("End time must be in the future and after start time");
      safeSetSubmitting(false);
      return;
    }
    const payload: AlertmanagerSilencePayloadT = {
      id:
        silenceFormStore.data.operation === "edit"
          ? silenceFormStore.data.silenceID || undefined
          : undefined,
      matchers: compiledMatchers,
      startsAt: startsAt.toISOString(),
      endsAt: submittedEndsAt.toISOString(),
      createdBy: author.trim(),
      comment: comment,
    };

    try {
      const validationResponse = await fetch(
        FormatBackendURI("silence-editor/preview"),
        {
          ...CommonOptions,
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            target,
            matchers: compiledMatchers,
            snapshot: previewSnapshot || undefined,
            limit: 1,
          } satisfies SilenceEditorPreviewRequestT),
        },
      );
      if (!isActiveGeneration()) {
        return;
      }
      if (!validationResponse.ok) {
        const validationPayload = (await validationResponse.json()) as {
          error?: string;
        };
        safeSetSubmitError(
          validationPayload.error || "Matcher validation failed",
        );
        if (validationResponse.status === 409) {
          setPreviewSnapshot(null);
          setMatcherPreflightValid(false);
          setPreviewRefreshNonce((value) => value + 1);
        }
        safeSetSubmitting(false);
        return;
      }
    } catch (err) {
      safeSetSubmitError(
        err instanceof Error
          ? `Matcher validation failed: ${err.message}`
          : "Matcher validation failed",
      );
      safeSetSubmitting(false);
      return;
    }

    try {
      const request: SilenceEditorSubmitRequestT = {
        target,
        operation: silenceFormStore.data.operation,
        originalSilenceID: silenceFormStore.data.silenceID ?? undefined,
        routeMember:
          silenceFormStore.data.operation === "create"
            ? undefined
            : silenceFormStore.data.editorRouteMember || undefined,
        revisionToken:
          silenceFormStore.data.operation === "create"
            ? undefined
            : silenceFormStore.data.editorRevisionToken || undefined,
        payload,
      };
      const proxyAbort = new AbortController();
      proxySubmitAbortRef.current = proxyAbort;
      const res = await fetch(FormatBackendURI("silence-editor/submit"), {
        ...CommonOptions,
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(request),
        signal: proxyAbort.signal,
      });
      if (!isActiveGeneration()) {
        return;
      }
      const responseText = await res.text();
      if (proxySubmitAbortRef.current === proxyAbort) {
        proxySubmitAbortRef.current = null;
      }
      if (!isActiveGeneration()) {
        return;
      }
      let parsedBody: unknown = null;
      if (responseText.trim() !== "") {
        try {
          parsedBody = JSON.parse(responseText);
        } catch {
          parsedBody = null;
        }
      }
      if (!res.ok) {
        const errorBody =
          parsedBody && typeof parsedBody === "object"
            ? (parsedBody as SilenceEditorErrorResponseT)
            : null;
        const message =
          errorBody?.error ||
          responseText.trim() ||
          `failed to submit silence: ${res.status}`;
        const shouldMarkUnknown =
          errorBody?.outcomeUnknown === true ||
          (errorBody === null && res.status >= 500);

        if (shouldMarkUnknown) {
          safeSetSubmitOutcomeUnknown(true);
          alertStore.status.forceNextFetch();
          safeSetSubmitError(
            `${message}. Outcome is unknown. Close this editor before starting another operation.`,
          );
        } else {
          if (errorBody?.requiresReopen === true) {
            safeSetSubmitRequiresReopen(true);
          }
          safeSetSubmitError(message);
        }
        safeSetSubmitting(false);
        return;
      }

      if (!parsedBody || typeof parsedBody !== "object") {
        safeSetSubmitOutcomeUnknown(true);
        alertStore.status.forceNextFetch();
        safeSetSubmitError(
          "failed to verify silence submission. Outcome is unknown. Close this editor before starting another operation.",
        );
        safeSetSubmitting(false);
        return;
      }

      const submitBody = parsedBody as SilenceEditorSubmitResponseT;
      const executeSuccess = (silenceID: string) => {
        if (!isActiveGeneration()) {
          return;
        }
        const shouldPersistAuthor =
          silenceFormStore.data.operation !== "edit" ||
          author !== initialAuthorRef.current;
        if (shouldPersistAuthor) {
          saveStoredAuthor(author.trim());
        }

        alertStore.status.forceNextFetch();
        silenceFormStore.data.setStart(startsAt);
        silenceFormStore.data.setEnd(submittedEndsAt);
        silenceFormStore.data.setAuthor(author.trim());
        silenceFormStore.data.setComment(comment);
        silenceFormStore.data.setAlertmanagers(
          AlertmanagerClustersToOption(mapTargetToMembers(target, alertStore)),
        );
        safeSetSubmitSuccess(silenceID);
        onSubmitted(target, silenceID);
        closeTimerRef.current = window.setTimeout(() => {
          if (isActiveGeneration()) {
            onClose();
          }
        }, 1000);
      };

      if (submitBody.mode === "direct") {
        const direct = submitBody.direct;
        if (!direct) {
          safeSetSubmitError(
            "failed to submit silence: missing direct submit descriptor",
          );
          safeSetSubmitting(false);
          return;
        }
        const correlationID = submitBody.correlationID?.trim();
        if (!correlationID) {
          console.error(SILENCE_EDITOR_CONSOLE_MESSAGE, {
            component: "silence_editor",
            event: "direct_descriptor_invalid",
            operation: silenceFormStore.data.operation,
            target,
            member: direct.member,
            mode: "browser_routed",
            reason_code: "missing_correlation_id",
          });
          safeSetSubmitError(
            "failed to submit silence: missing direct submit correlation ID",
          );
          safeSetSubmitting(false);
          return;
        }
        const audit = browserSubmitAuditBase(
          correlationID,
          silenceFormStore.data.operation,
          target,
          direct.member,
        );
        const directStartedAt = performance.now();
        console.info(SILENCE_EDITOR_CONSOLE_MESSAGE, {
          ...audit,
          event: "upstream_write_started",
        });

        const directAbort = new AbortController();
        directSubmitAbortRef.current = directAbort;
        let directTimedOut = false;
        let directFailureStage = "request";
        const directTimeout = window.setTimeout(() => {
          directTimedOut = true;
          directAbort.abort();
        }, directSubmitTimeoutMs);
        try {
          const directResponse = await fetch(direct.endpoint, {
            ...CommonOptions,
            method: "POST",
            credentials: direct.corsCredentials,
            headers: {
              "Content-Type": "application/json",
              ...(direct.headers || {}),
            },
            body: JSON.stringify(direct.payload),
            signal: directAbort.signal,
          });
          directFailureStage = "read_body";
          const text = await directResponse.text();
          directFailureStage = "decode_body";
          if (!directResponse.ok) {
            if (directResponse.status >= 500) {
              console.error(SILENCE_EDITOR_CONSOLE_MESSAGE, {
                ...audit,
                event: "upstream_write_outcome_unknown",
                http_status: directResponse.status,
                failure_stage: "upstream_5xx",
                duration_ms: browserSubmitDuration(directStartedAt),
              });
              if (!isActiveGeneration()) {
                return;
              }
              safeSetSubmitOutcomeUnknown(true);
              alertStore.status.forceNextFetch();
              safeSetSubmitError(
                `failed to submit silence via ${direct.member}. Outcome is unknown. Close this editor before starting another operation.`,
              );
            } else {
              console.warn(SILENCE_EDITOR_CONSOLE_MESSAGE, {
                ...audit,
                event: "upstream_write_rejected",
                http_status: directResponse.status,
                reason_code: "upstream_rejected",
                duration_ms: browserSubmitDuration(directStartedAt),
              });
              if (!isActiveGeneration()) {
                return;
              }
              safeSetSubmitError(
                text.trim() ||
                  `failed to submit silence via ${direct.member}: ${directResponse.status}`,
              );
            }
            safeSetSubmitting(false);
            return;
          }

          let directPayload: { silenceID?: string } = {};
          try {
            directPayload = text.trim()
              ? (JSON.parse(text) as { silenceID?: string })
              : {};
          } catch {
            console.error(SILENCE_EDITOR_CONSOLE_MESSAGE, {
              ...audit,
              event: "upstream_write_outcome_unknown",
              http_status: directResponse.status,
              failure_stage: "decode_body",
              duration_ms: browserSubmitDuration(directStartedAt),
            });
            if (!isActiveGeneration()) {
              return;
            }
            safeSetSubmitOutcomeUnknown(true);
            alertStore.status.forceNextFetch();
            safeSetSubmitError(
              `failed to verify silence submission via ${direct.member}. Outcome is unknown. Close this editor before starting another operation.`,
            );
            safeSetSubmitting(false);
            return;
          }

          const silenceID = directPayload.silenceID;
          if (!silenceID) {
            console.error(SILENCE_EDITOR_CONSOLE_MESSAGE, {
              ...audit,
              event: "upstream_write_outcome_unknown",
              http_status: directResponse.status,
              failure_stage: "missing_id",
              duration_ms: browserSubmitDuration(directStartedAt),
            });
            if (!isActiveGeneration()) {
              return;
            }
            safeSetSubmitOutcomeUnknown(true);
            alertStore.status.forceNextFetch();
            safeSetSubmitError(
              `failed to verify silence submission via ${direct.member}. Outcome is unknown. Close this editor before starting another operation.`,
            );
            safeSetSubmitting(false);
            return;
          }

          console.info(SILENCE_EDITOR_CONSOLE_MESSAGE, {
            ...audit,
            event: "upstream_write_succeeded",
            http_status: directResponse.status,
            silence_id: silenceID,
            duration_ms: browserSubmitDuration(directStartedAt),
          });
          executeSuccess(silenceID);
          return;
        } catch (err) {
          const failureStage = directTimedOut
            ? "timeout"
            : directAbort.signal.aborted
              ? "aborted"
              : directFailureStage;
          console.error(SILENCE_EDITOR_CONSOLE_MESSAGE, {
            ...audit,
            event: "upstream_write_outcome_unknown",
            failure_stage: failureStage,
            duration_ms: browserSubmitDuration(directStartedAt),
          });
          if (!isActiveGeneration()) {
            return;
          }
          safeSetSubmitOutcomeUnknown(true);
          alertStore.status.forceNextFetch();
          safeSetSubmitError(
            directTimedOut
              ? `Silence submission via ${direct.member} timed out. Outcome is unknown. Close this editor before starting another operation.`
              : err instanceof Error
                ? `${err.message}. Outcome is unknown. Close this editor before starting another operation.`
                : "Outcome is unknown. Close this editor before starting another operation.",
          );
          safeSetSubmitting(false);
          return;
        } finally {
          window.clearTimeout(directTimeout);
          if (directSubmitAbortRef.current === directAbort) {
            directSubmitAbortRef.current = null;
          }
        }
      }

      const silenceID = submitBody.silenceID;
      if (!silenceID) {
        safeSetSubmitOutcomeUnknown(true);
        alertStore.status.forceNextFetch();
        safeSetSubmitError(
          "failed to verify silence submission. Outcome is unknown. Close this editor before starting another operation.",
        );
        safeSetSubmitting(false);
        return;
      }

      executeSuccess(silenceID);
    } catch (err) {
      safeSetSubmitOutcomeUnknown(true);
      alertStore.status.forceNextFetch();
      safeSetSubmitError(
        err instanceof Error
          ? `${err.message}. Outcome is unknown. Close this editor before starting another operation.`
          : "Outcome is unknown. Close this editor before starting another operation.",
      );
      safeSetSubmitting(false);
      return;
    }
  };

  const readOnly = submitting || submitSuccess !== null;
  return (
    <div className="fork-silence-editor">
      <div className="fork-silence-editor__body">
        <div className="fork-silence-editor__grid">
          <div className="fork-silence-editor__field">
            <label htmlFor={`${editorFieldId}-target`}>Alertmanager</label>
            <select
              id={`${editorFieldId}-target`}
              data-modal-initial-focus={
                silenceFormStore.data.operation === "create" ? true : undefined
              }
              className="form-select"
              value={target}
              disabled={
                readOnly || silenceFormStore.data.operation !== "create"
              }
              onChange={(e) => setTarget(e.target.value)}
            >
              {defaultTargets.map((t) => (
                <option key={t} value={t}>
                  {t}
                </option>
              ))}
            </select>
          </div>

          <div className="fork-silence-editor__field">
            <label htmlFor={`${editorFieldId}-starts-mode`}>Starts</label>
            <select
              id={`${editorFieldId}-starts-mode`}
              data-modal-initial-focus={
                silenceFormStore.data.operation !== "create" ? true : undefined
              }
              className="form-select"
              value={startMode}
              disabled={readOnly}
              onChange={(e) => {
                const next = e.target.value as "now" | "date";
                setStartMode(next);
              }}
            >
              <option value="now">Now</option>
              <option value="date">At date/time...</option>
            </select>
            {startMode === "date" ? (
              <div className="fork-silence-editor__timing-row">
                <input
                  id={`${editorFieldId}-starts-date`}
                  aria-label="Start date"
                  type="date"
                  className="form-control"
                  value={startDate}
                  disabled={readOnly}
                  onChange={(e) => {
                    setStartDate(e.target.value);
                  }}
                />
                <input
                  id={`${editorFieldId}-starts-time`}
                  aria-label="Start time"
                  type="time"
                  className="form-control"
                  value={startTime}
                  disabled={readOnly}
                  onChange={(e) => {
                    setStartTime(e.target.value);
                  }}
                />
              </div>
            ) : (
              <small className="fork-silence-editor__timing-summary">
                <strong>At submission</strong>
                <br />
                Estimate: {formatUTCTimestamp(startAtBase || estimateNow)}
              </small>
            )}
            {startMode === "date" && startAtBase ? (
              <small className="fork-silence-editor__timing-summary">
                <strong>{formatLocalTimestamp(startAtBase)}</strong>
                <br />
                {formatUTCTimestamp(startAtBase)}
              </small>
            ) : null}
          </div>

          <div className="fork-silence-editor__field">
            <label htmlFor={`${editorFieldId}-ends-mode`}>Ends</label>
            <select
              id={`${editorFieldId}-ends-mode`}
              className="form-select"
              value={
                durationKind === "preset"
                  ? `preset:${presetDuration}`
                  : durationKind === "custom"
                    ? "custom"
                    : "absolute"
              }
              disabled={readOnly}
              onChange={(e) => {
                const value = e.target.value;
                if (value === "custom") {
                  setDurationKind("custom");
                } else if (value === "absolute") {
                  setDurationKind("absolute");
                } else {
                  setDurationKind("preset");
                  setPresetDuration(value.replace("preset:", ""));
                }
              }}
            >
              {PRESET_DURATIONS.map((p) => (
                <option key={p.value} value={`preset:${p.value}`}>
                  {p.label}
                </option>
              ))}
              <option value="custom">Custom duration...</option>
              <option value="absolute">At date/time...</option>
            </select>
            {durationKind === "custom" ? (
              <input
                id={`${editorFieldId}-custom-duration`}
                aria-label="Custom duration"
                type="text"
                className="form-control"
                value={customDuration}
                disabled={readOnly}
                onChange={(e) => {
                  setCustomDuration(e.target.value);
                }}
                placeholder="1w2d6h or 1y"
              />
            ) : null}
            {durationKind === "absolute" ? (
              <div className="fork-silence-editor__timing-row">
                <input
                  id={`${editorFieldId}-ends-date`}
                  aria-label="End date"
                  type="date"
                  className="form-control"
                  value={endDate}
                  disabled={readOnly}
                  onChange={(e) => {
                    setEndDate(e.target.value);
                  }}
                />
                <input
                  id={`${editorFieldId}-ends-time`}
                  aria-label="End time"
                  type="time"
                  className="form-control"
                  value={endTime}
                  disabled={readOnly}
                  onChange={(e) => {
                    setEndTime(e.target.value);
                  }}
                />
              </div>
            ) : null}
            {timeError ? (
              <small className="fork-silence-editor__timing-summary text-danger">
                <strong>{timeError}</strong>
              </small>
            ) : endsAt ? (
              <small className="fork-silence-editor__timing-summary">
                <strong>{formatLocalTimestamp(endsAt)}</strong>
                <br />
                {durationKind !== "absolute" ? "Estimate: " : ""}
                {formatUTCTimestamp(endsAt)}
              </small>
            ) : null}
          </div>

          <div className="fork-silence-editor__field fork-silence-editor__full">
            <span className="fork-silence-editor__field-label">Matchers</span>
            <div className="fork-silence-editor__matchers">
              {expressions.map((expression, index) => (
                <div
                  className={`fork-silence-editor__matcher-row${
                    editingMatcher === index ? " editing" : ""
                  }`}
                  key={`matcher-${index}`}
                >
                  <div
                    className={`fork-silence-editor__matcher-control${
                      parseMatcherExpression(expression) ? "" : " invalid"
                    }`}
                    style={
                      {
                        "--matcher-width": `${Math.max(
                          15,
                          Math.min(52, expression.length + 1),
                        )}ch`,
                      } as CSSProperties
                    }
                  >
                    <div
                      className="fork-silence-editor__matcher-highlight"
                      aria-hidden="true"
                    >
                      <MatcherSyntax expression={expression} />
                    </div>
                    <input
                      ref={(input) => {
                        matcherInputsRef.current[index] = input;
                      }}
                      className="fork-silence-editor__matcher-expression"
                      value={expression}
                      title={expression}
                      placeholder='label="value"'
                      autoComplete="off"
                      spellCheck={false}
                      disabled={readOnly}
                      readOnly={editingMatcher !== index}
                      role="combobox"
                      aria-autocomplete="list"
                      aria-label={`Matcher ${index + 1} expression, ${
                        editingMatcher === index ? "editing" : "resting"
                      }`}
                      aria-controls={`${matcherComboboxId}-list-${index}`}
                      aria-expanded={
                        editingMatcher === index &&
                        suggestions !== null &&
                        suggestions.index === index
                      }
                      aria-activedescendant={
                        editingMatcher === index &&
                        suggestions !== null &&
                        suggestions.index === index &&
                        !suggestions.loading &&
                        suggestions.items[activeSuggestionIndex]
                          ? `${matcherComboboxId}-option-${index}-${activeSuggestionIndex}`
                          : undefined
                      }
                      onFocus={(event) => {
                        if (editingMatcher !== index) {
                          matcherEditBackupRef.current[index] = expression;
                          setEditingMatcher(index);
                          setActiveSuggestionIndex(0);
                        }
                        void fetchSuggestions(
                          index,
                          expression,
                          event.currentTarget.selectionStart || 0,
                        );
                      }}
                      onBlur={() => {
                        window.setTimeout(() => {
                          if (suggestionPointerDownRef.current) {
                            suggestionPointerDownRef.current = false;
                            return;
                          }
                          setSuggestions(null);
                          if (parseMatcherExpression(expressions[index])) {
                            setEditingMatcher((current) =>
                              current === index ? null : current,
                            );
                          }
                        }, 100);
                      }}
                      onChange={(event) => {
                        const next = [...expressions];
                        next[index] = event.target.value;
                        setExpressions(next);
                        suggestionGenerationRef.current++;
                        if (suggestionDebounceRef.current !== null) {
                          window.clearTimeout(suggestionDebounceRef.current);
                          suggestionDebounceRef.current = null;
                        }
                        if (suggestionAbortRef.current !== null) {
                          suggestionAbortRef.current.abort();
                          suggestionAbortRef.current = null;
                        }
                        setSuggestions(null);
                        setActiveSuggestionIndex(0);
                      }}
                      onScroll={(event) => {
                        const highlight = event.currentTarget
                          .previousElementSibling as HTMLElement | null;
                        if (highlight) {
                          highlight.style.transform = `translateX(${-event.currentTarget.scrollLeft}px)`;
                        }
                      }}
                      onKeyDown={(event) => {
                        const available =
                          suggestions?.index === index && !suggestions.loading
                            ? suggestions.items
                            : [];
                        if (event.key === "ArrowDown" && available.length > 0) {
                          event.preventDefault();
                          setActiveSuggestionIndex(
                            (activeSuggestionIndex + 1) % available.length,
                          );
                        } else if (
                          event.key === "ArrowUp" &&
                          available.length > 0
                        ) {
                          event.preventDefault();
                          setActiveSuggestionIndex(
                            (activeSuggestionIndex - 1 + available.length) %
                              available.length,
                          );
                        } else if (event.key === "Enter") {
                          event.preventDefault();
                          if (available.length > 0) {
                            applySuggestion(
                              index,
                              available[activeSuggestionIndex],
                            );
                          } else if (parseMatcherExpression(expression)) {
                            setSuggestions(null);
                            setEditingMatcher(null);
                          }
                        } else if (event.key === "Escape") {
                          event.preventDefault();
                          event.stopPropagation();
                          const next = [...expressions];
                          next[index] =
                            matcherEditBackupRef.current[index] ?? expression;
                          setExpressions(next);
                          setSuggestions(null);
                          setEditingMatcher(null);
                        }
                      }}
                      onKeyUp={(event) => {
                        if (
                          ["ArrowDown", "ArrowUp", "Enter", "Escape"].includes(
                            event.key,
                          )
                        ) {
                          return;
                        }
                        void fetchSuggestions(
                          index,
                          event.currentTarget.value,
                          event.currentTarget.selectionStart || 0,
                        );
                      }}
                    />
                  </div>
                  <button
                    type="button"
                    className="fork-silence-editor__matcher-remove"
                    disabled={readOnly || expressions.length <= 1}
                    aria-label={`Remove matcher ${index + 1}`}
                    onClick={() => removeMatcherAt(index)}
                  >
                    x
                  </button>
                  {editingMatcher === index &&
                  suggestions &&
                  suggestions.index === index ? (
                    <div
                      ref={(list) => {
                        suggestionListsRef.current[index] = list;
                      }}
                      id={`${matcherComboboxId}-list-${index}`}
                      className="fork-silence-editor__suggestions"
                      role="listbox"
                      style={suggestionMenuStyle}
                      onPointerDown={() => {
                        suggestionPointerDownRef.current = true;
                      }}
                      onPointerUp={() => {
                        window.setTimeout(() => {
                          suggestionPointerDownRef.current = false;
                        }, 150);
                      }}
                      onPointerCancel={() => {
                        window.setTimeout(() => {
                          suggestionPointerDownRef.current = false;
                        }, 150);
                      }}
                    >
                      {suggestions.loading ? (
                        <div className="fork-silence-editor__suggestion">
                          Loading...
                        </div>
                      ) : suggestions.items.length === 0 ? (
                        <div className="fork-silence-editor__suggestion">
                          No suggestions
                        </div>
                      ) : (
                        suggestions.items.map((item, suggestionIndex) => (
                          <button
                            id={`${matcherComboboxId}-option-${index}-${suggestionIndex}`}
                            type="button"
                            key={item.text}
                            className={`fork-silence-editor__suggestion${
                              suggestionIndex === activeSuggestionIndex
                                ? " active"
                                : ""
                            }`}
                            role="option"
                            aria-selected={
                              suggestionIndex === activeSuggestionIndex
                            }
                            onMouseDown={(event) => event.preventDefault()}
                            onMouseEnter={() => {
                              setActiveSuggestionIndex(suggestionIndex);
                            }}
                            onClick={() => applySuggestion(index, item)}
                          >
                            <span>{item.text}</span>
                            <small>{item.meta}</small>
                          </button>
                        ))
                      )}
                      {suggestions.summary ? (
                        <div className="fork-silence-editor__suggestion-summary">
                          {suggestions.summary}
                        </div>
                      ) : null}
                    </div>
                  ) : null}
                </div>
              ))}
            </div>
            <button
              type="button"
              className="btn btn-success btn-sm mt-2"
              disabled={readOnly}
              onClick={() => {
                const index = expressions.length;
                matcherEditBackupRef.current[index] = "";
                setExpressions([...expressions, ""]);
                setEditingMatcher(index);
                setActiveSuggestionIndex(0);
              }}
            >
              + Add matcher
            </button>
            {matcherError ? (
              <div className="text-danger small mt-1">{matcherError}</div>
            ) : null}
          </div>

          <div className="fork-silence-editor__field">
            <label htmlFor={`${editorFieldId}-author`}>Author</label>
            <input
              id={`${editorFieldId}-author`}
              className="form-control"
              value={author}
              disabled={readOnly}
              onChange={(e) => setAuthor(e.target.value)}
            />
          </div>

          <div className="fork-silence-editor__field fork-silence-editor__full">
            <label htmlFor={`${editorFieldId}-comment`}>Comment</label>
            <textarea
              id={`${editorFieldId}-comment`}
              className="form-control"
              value={comment}
              disabled={readOnly}
              onChange={(e) => setComment(e.target.value)}
              rows={3}
            />
          </div>
        </div>

        <div className="fork-silence-editor__preview">
          <div className="fork-silence-editor__preview-header">
            <strong>Affected alerts</strong>
            <span className="badge bg-secondary ms-2">{previewTotal}</span>
            {previewRefreshing ? (
              <span className="ms-2 text-muted">
                <FontAwesomeIcon icon={faSpinner} spin /> Refreshing
              </span>
            ) : null}
            {previewStale ? (
              <span className="badge bg-warning text-dark ms-2">Stale</span>
            ) : null}
          </div>
          {previewError ? (
            <div className="text-danger small p-2">{previewError}</div>
          ) : null}
          {(previewError === null || preview.length > 0) && (
            <div className="fork-silence-editor__preview-list">
              {preview.map((alert) => {
                const stateKey =
                  alert.state === "suppressed" && !alert.isLive
                    ? "suppressed-past"
                    : alert.state;
                const stateClass =
                  BackgroundClassMap[
                    stateKey as keyof typeof BackgroundClassMap
                  ];
                const matcherTitle = (name: string) =>
                  expressions
                    .filter(
                      (expression) =>
                        parseMatcherExpression(expression)?.name === name,
                    )
                    .join("\n");
                return (
                  <div
                    className="fork-silence-editor__preview-item"
                    key={alert.id}
                  >
                    <span className={`badge ${stateClass} me-2`}>
                      {stateKey}
                    </span>
                    {alert.labels.map((label) => (
                      <span
                        key={`${alert.id}:${label.name}:${label.value}`}
                        className={`badge me-1 fork-silence-editor__preview-label ${
                          alert.matchedLabelNames.includes(label.name)
                            ? "fork-silence-editor__preview-label--matched"
                            : ""
                        }`}
                        title={matcherTitle(label.name)}
                      >
                        {label.name}={label.value}
                      </span>
                    ))}
                    {alert.missingLabelNames.map((name) => (
                      <span
                        key={`${alert.id}:missing:${name}`}
                        className="badge me-1 fork-silence-editor__preview-label fork-silence-editor__preview-label--missing fork-silence-editor__missing-label"
                        title={matcherTitle(name)}
                      >
                        {name}={"<missing>"}
                      </span>
                    ))}
                    {alert.matchedLabels
                      .filter(
                        (matched) =>
                          !alert.labels.some(
                            (label) => label.name === matched.name,
                          ),
                      )
                      .map((matched) => (
                        <span
                          key={`${alert.id}:matched-only:${matched.name}:${matched.value}`}
                          className="badge me-1 fork-silence-editor__preview-label fork-silence-editor__preview-label--matched-only"
                          title={matcherTitle(matched.name)}
                        >
                          {matched.name}={matched.value}
                        </span>
                      ))}
                  </div>
                );
              })}
              {previewError === null && preview.length === 0 ? (
                <div className="text-muted small p-2">No affected alerts</div>
              ) : null}
              {previewNextCursor ? (
                <div className="p-2 text-center">
                  <button
                    type="button"
                    className="btn btn-outline-secondary btn-sm"
                    disabled={previewRefreshing || readOnly || previewStale}
                    onClick={() => {
                      void loadMorePreview();
                    }}
                  >
                    Load more
                  </button>
                </div>
              ) : null}
            </div>
          )}
          {previewExcludedNoRawLabels > 0 ? (
            <div className="small text-muted p-2">
              Excluded alerts without raw labels: {previewExcludedNoRawLabels}
            </div>
          ) : null}
        </div>

        {submitError ? (
          <div className="alert alert-danger mt-2 mb-0">{submitError}</div>
        ) : null}
        {submitSuccess ? (
          <div className="alert alert-success mt-2 mb-0">
            Silence submitted to <code>{target}</code>:{" "}
            <code>{submitSuccess}</code>
          </div>
        ) : null}

        <div className="fork-silence-editor__actions mt-3">
          <button
            type="button"
            className="btn fork-silence-editor__cancel"
            disabled={readOnly}
            onClick={onClose}
          >
            Cancel
          </button>
          <button
            type="button"
            className="btn btn-primary"
            disabled={
              readOnly ||
              formError !== null ||
              submitOutcomeUnknown ||
              submitRequiresReopen ||
              silenceFormStore.data.editorSessionState !== "editing" ||
              !matcherPreflightValid
            }
            onClick={() => {
              void submit();
            }}
          >
            {submitting ? (
              <>
                <FontAwesomeIcon icon={faSpinner} spin className="me-1" />
                Submitting...
              </>
            ) : submitError ? (
              "Retry"
            ) : silenceFormStore.data.operation === "edit" ? (
              "Update silence"
            ) : silenceFormStore.data.operation === "recreate" ? (
              "Recreate silence"
            ) : (
              "Create silence"
            )}
          </button>
        </div>
      </div>
    </div>
  );
};

export default observer(SilenceEditor);
