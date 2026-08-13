import {
  SilenceEditorSessionEventT,
  SilenceEditorSessionStateT,
  transitionSilenceEditorSession,
} from "./SilenceEditorSession";

const run = (
  events: SilenceEditorSessionEventT[],
  initial: SilenceEditorSessionStateT = "closed",
): SilenceEditorSessionStateT =>
  events.reduce(transitionSilenceEditorSession, initial);

describe("SilenceEditorSession", () => {
  it("exhausts every state/event pair against the canonical transition table", () => {
    const states: SilenceEditorSessionStateT[] = [
      "closed",
      "checking",
      "editing",
      "submitting",
      "success",
      "outcomeUnknown",
    ];
    const events: SilenceEditorSessionEventT[] = [
      "BEGIN_CREATE",
      "BEGIN_CHECK",
      "CHECK_SUCCESS",
      "CHECK_FAILURE",
      "BEGIN_SUBMIT",
      "SUBMIT_SUCCESS",
      "SUBMIT_KNOWN_FAILURE",
      "SUBMIT_UNKNOWN",
      "CLOSE",
    ];
    const allowed: Partial<
      Record<
        SilenceEditorSessionStateT,
        Partial<Record<SilenceEditorSessionEventT, SilenceEditorSessionStateT>>
      >
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

    for (const state of states) {
      for (const event of events) {
        expect(transitionSilenceEditorSession(state, event)).toBe(
          allowed[state]?.[event] ?? state,
        );
      }
    }
  });

  it("opens the editor only after status succeeds", () => {
    expect(run(["BEGIN_CHECK", "CHECK_FAILURE"])).toBe("closed");
    expect(run(["BEGIN_CHECK", "CHECK_SUCCESS"])).toBe("editing");
  });

  it("blocks navigation transitions while submitting", () => {
    expect(run(["BEGIN_CREATE", "BEGIN_SUBMIT", "BEGIN_CHECK", "CLOSE"])).toBe(
      "submitting",
    );
  });

  it("makes unknown outcome terminal until close", () => {
    expect(
      run(["BEGIN_CREATE", "BEGIN_SUBMIT", "SUBMIT_UNKNOWN", "BEGIN_CREATE"]),
    ).toBe("outcomeUnknown");
    expect(
      run([
        "BEGIN_CREATE",
        "BEGIN_SUBMIT",
        "SUBMIT_UNKNOWN",
        "BEGIN_SUBMIT",
      ]),
    ).toBe("outcomeUnknown");
    expect(
      run(["BEGIN_CREATE", "BEGIN_SUBMIT", "SUBMIT_UNKNOWN", "CLOSE"]),
    ).toBe("closed");
  });
});
