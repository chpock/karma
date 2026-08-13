export type SilenceEditorSessionStateT =
  | "closed"
  | "checking"
  | "editing"
  | "submitting"
  | "success"
  | "outcomeUnknown";

export type SilenceEditorSessionEventT =
  | "BEGIN_CREATE"
  | "BEGIN_CHECK"
  | "CHECK_SUCCESS"
  | "CHECK_FAILURE"
  | "BEGIN_SUBMIT"
  | "SUBMIT_SUCCESS"
  | "SUBMIT_KNOWN_FAILURE"
  | "SUBMIT_UNKNOWN"
  | "CLOSE";

const transitions: Record<
  SilenceEditorSessionStateT,
  Partial<Record<SilenceEditorSessionEventT, SilenceEditorSessionStateT>>
> = {
  closed: {
    BEGIN_CREATE: "editing",
    BEGIN_CHECK: "checking",
    CLOSE: "closed",
  },
  checking: {
    BEGIN_CHECK: "checking",
    CHECK_SUCCESS: "editing",
    CHECK_FAILURE: "closed",
    CLOSE: "closed",
  },
  editing: {
    BEGIN_CHECK: "checking",
    BEGIN_SUBMIT: "submitting",
    CLOSE: "closed",
  },
  submitting: {
    SUBMIT_SUCCESS: "success",
    SUBMIT_KNOWN_FAILURE: "editing",
    SUBMIT_UNKNOWN: "outcomeUnknown",
  },
  success: { CLOSE: "closed" },
  outcomeUnknown: {
    CLOSE: "closed",
  },
};

export function transitionSilenceEditorSession(
  state: SilenceEditorSessionStateT,
  event: SilenceEditorSessionEventT,
): SilenceEditorSessionStateT {
  return transitions[state][event] ?? state;
}
