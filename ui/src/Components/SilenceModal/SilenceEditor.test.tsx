import {
  act,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";

import { AlertStore, NewUnappliedFilter } from "Stores/AlertStore";
import { SilenceFormStore, NewEmptyMatcher } from "Stores/SilenceFormStore";
import { Settings } from "Stores/Settings";
import { StringToOption } from "Common/Select";
import { QueryOperators } from "Common/Query";
import type { AlertmanagerSilencePayloadT } from "Models/APITypes";
import SilenceEditor from "./SilenceEditor";
import type { SilenceEditorCloseReasonT } from "./index";

const createStores = (editMode = false) => {
  const alertStore = new AlertStore([]);
  const silenceFormStore = new SilenceFormStore();
  const settingsStore = new Settings(null);

  alertStore.data.setUpstreams({
    counters: { total: 1, healthy: 1, failed: 0 },
    clusters: { am1: ["am1"] },
    instances: [
      {
        name: "am1",
        uri: "http://am1.example.com",
        publicURI: "http://am1.example.com",
        readonly: false,
        headers: {},
        corsCredentials: "include",
        error: "",
        warnings: [],
        version: "0.24.0",
        cluster: "am1",
        clusterMembers: ["am1"],
      },
    ],
  });

  const matcher = NewEmptyMatcher();
  matcher.name = "alertname";
  matcher.values = [StringToOption("Host_Down")];
  silenceFormStore.data.setMatchers([matcher]);
  silenceFormStore.data.setAutofillMatchers(false);
  silenceFormStore.data.setAuthor("me@example.com");
  silenceFormStore.data.setComment("test silence");

  if (editMode) {
    silenceFormStore.data.setSilenceID("silence-1");
    silenceFormStore.data.setStart(new Date("2026-01-02T10:00:00Z"));
    silenceFormStore.data.setEnd(new Date("2026-01-02T13:00:00Z"));
  }

  return { alertStore, silenceFormStore, settingsStore };
};

const previewResponse = (
  alerts: Array<Record<string, unknown>> = [],
  nextCursor?: string,
  total = alerts.length,
) =>
  new Response(
    JSON.stringify({
      status: "success",
      target: "am1",
      total,
      returned: alerts.length,
      limit: 100,
      nextCursor,
      excludedNoRawLabels: 0,
      alerts,
    }),
    { status: 200, headers: { "Content-Type": "application/json" } },
  );

const previewAlert = (id: string) => ({
  id,
  state: "active",
  rawState: "active",
  isLive: true,
  isSuppressed: false,
  isResolved: false,
  startsAt: "2026-01-01T00:00:00Z",
  labels: [{ name: "alertname", value: id }],
  matchedLabels: [{ name: "alertname", value: id }],
  matchedLabelNames: ["alertname"],
  missingLabelNames: [],
});

const parseSubmitPayload = (init?: RequestInit) => {
  const req = JSON.parse((init?.body as string) || "{}") as {
    target?: string;
    payload?: Record<string, unknown>;
  };
  return req.payload as Record<string, unknown>;
};

const renderEditor = (
  stores: ReturnType<typeof createStores>,
  options: {
    onClose?: () => void;
    onSubmitted?: (target: string, silenceID: string) => void;
    registerCloseGuard?: (
      guard: (reason: SilenceEditorCloseReasonT) => boolean,
    ) => void;
  } = {},
) =>
  render(
    <SilenceEditor
      {...stores}
      onClose={options.onClose || (() => {})}
      onSubmitted={options.onSubmitted || (() => {})}
      registerCloseGuard={options.registerCloseGuard || (() => {})}
    />,
  );

const fillRequiredMetadata = (
  author = "me@example.com",
  comment = "test silence",
) => {
  const authorField = screen
    .getByText("Author")
    .parentElement?.querySelector("input");
  if (authorField) {
    fireEvent.change(authorField, { target: { value: author } });
  }
  const commentField = screen
    .getByText("Comment")
    .parentElement?.querySelector("textarea");
  if (commentField) {
    fireEvent.change(commentField, { target: { value: comment } });
  }
};

describe("<SilenceEditor />", () => {
  const originalFetch = global.fetch;

  afterEach(() => {
    jest.restoreAllMocks();
    global.fetch = originalFetch;
    jest.useRealTimers();
    localStorage.clear();
  });

  it("enforces strict custom-duration grammar (Q9b)", async () => {
    const { alertStore, silenceFormStore, settingsStore } = createStores(false);

    global.fetch = jest.fn(async (input: RequestInfo | URL) => {
      const url = input.toString();
      if (url.includes("/silence-editor/preview")) {
        return new Response(
          JSON.stringify({
            status: "success",
            target: "am1",
            total: 0,
            returned: 0,
            limit: 100,
            excludedNoRawLabels: 0,
            alerts: [],
          }),
          {
            status: 200,
            headers: { "Content-Type": "application/json" },
          },
        );
      }
      return new Response(JSON.stringify({ silenceID: "s-1" }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    }) as typeof fetch;

    const { container } = render(
      <SilenceEditor
        alertStore={alertStore}
        silenceFormStore={silenceFormStore}
        settingsStore={settingsStore}
        onClose={() => {}}
        onSubmitted={() => {}}
        registerCloseGuard={() => {}}
      />,
    );

    const selects = container.querySelectorAll("select");
    fireEvent.change(selects[2], { target: { value: "custom" } });
    const durationInput = screen.getByPlaceholderText("1w2d6h or 1y");

    const submit = screen.getByRole("button", { name: "Create silence" });
    expect(submit).toHaveClass("fork-silence-editor__submit");
    expect(screen.getByText("Author is required.")).toHaveClass(
      "fork-silence-editor__field-error",
    );
    expect(submit).toHaveAttribute(
      "aria-describedby",
      expect.stringMatching(/author-error/),
    );

    for (const invalid of ["30m2h", "0h1m", "1h2h", "01h", " 1h "]) {
      fireEvent.change(durationInput, { target: { value: invalid } });
      await waitFor(() => {
        expect(submit).toBeDisabled();
      });
    }

    fireEvent.change(durationInput, { target: { value: "1w2d6h30m" } });
    fillRequiredMetadata();
    await waitFor(() => {
      expect(submit).toBeEnabled();
    });
    expect(submit).not.toHaveAttribute("aria-describedby");
  });

  it("anchors duration to max(startsAt, submitTime) in edit mode (Q7c)", async () => {
    jest.useFakeTimers();
    jest.setSystemTime(new Date("2026-01-02T12:00:00Z"));

    const { alertStore, silenceFormStore, settingsStore } = createStores(true);
    const requests: string[] = [];
    let submitPayload: any = null;

    global.fetch = jest.fn(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = input.toString();
        requests.push(url);
        if (url.includes("/silence-editor/preview")) {
          return new Response(
            JSON.stringify({
              status: "success",
              target: "am1",
              total: 0,
              returned: 0,
              limit: 100,
              excludedNoRawLabels: 0,
              alerts: [],
            }),
            {
              status: 200,
              headers: { "Content-Type": "application/json" },
            },
          );
        }

        submitPayload = parseSubmitPayload(init);
        return new Response(JSON.stringify({ silenceID: "s-2" }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        });
      },
    ) as typeof fetch;

    render(
      <SilenceEditor
        alertStore={alertStore}
        silenceFormStore={silenceFormStore}
        settingsStore={settingsStore}
        onClose={() => {}}
        onSubmitted={() => {}}
        registerCloseGuard={() => {}}
      />,
    );

    const selects = screen.getAllByRole("combobox");
    fireEvent.change(selects[2], { target: { value: "preset:1h" } });

    await act(async () => {
      fireEvent.click(screen.getByRole("button", { name: "Update silence" }));
    });

    await waitFor(() => {
      expect(requests.some((u) => u.includes("/silence-editor/submit"))).toBe(
        true,
      );
    });

    const nowMs = Date.now();
    const startsAtMs = Date.parse(submitPayload.startsAt);
    const endsAtMs = Date.parse(submitPayload.endsAt);

    expect(startsAtMs).toBeLessThan(nowMs - 60 * 60 * 1000);
    expect(endsAtMs).toBeGreaterThanOrEqual(nowMs + 55 * 60 * 1000);
    expect(endsAtMs).toBeLessThanOrEqual(nowMs + 65 * 60 * 1000);
    expect(endsAtMs - startsAtMs).toBeGreaterThan(2 * 60 * 60 * 1000);
  });

  it("uses the logical cluster ID for a preselected physical member", () => {
    const stores = createStores(false);
    stores.alertStore.data.setUpstreams({
      counters: { total: 1, healthy: 1, failed: 0 },
      clusters: { production: ["am1"] },
      instances: [
        {
          name: "am1",
          uri: "http://am1.example.com",
          publicURI: "http://am1.example.com",
          readonly: false,
          headers: {},
          corsCredentials: "include",
          error: "",
          warnings: [],
          version: "0.24.0",
          cluster: "production",
          clusterMembers: ["am1"],
        },
      ],
    });
    stores.silenceFormStore.data.setAlertmanagers([
      { label: "am1", value: ["am1"] },
    ]);

    renderEditor(stores);

    expect(screen.getAllByRole("combobox")[0]).toHaveValue("production");
  });

  it("preserves exact startsAt and endsAt when editing without timing changes", async () => {
    jest.useFakeTimers();
    jest.setSystemTime(new Date("2026-01-02T12:00:00Z"));
    const stores = createStores(true);
    stores.silenceFormStore.data.setStart(new Date("2026-01-02T10:00:11.789Z"));
    stores.silenceFormStore.data.setEnd(new Date("2026-01-02T13:45:59.321Z"));
    let submitPayload: Record<string, unknown> | null = null;
    global.fetch = jest.fn(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        if (input.toString().includes("/silence-editor/preview")) {
          return previewResponse();
        }
        submitPayload = parseSubmitPayload(init);
        return new Response(JSON.stringify({ silenceID: "silence-1" }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        });
      },
    ) as typeof fetch;

    renderEditor(stores);
    expect(screen.getAllByRole("combobox")[2]).toHaveValue("absolute");

    await act(async () => {
      fireEvent.click(screen.getByRole("button", { name: "Update silence" }));
    });

    expect(submitPayload).toMatchObject({
      id: "silence-1",
      startsAt: "2026-01-02T10:00:11.789Z",
      endsAt: "2026-01-02T13:45:59.321Z",
    });
  });

  it("allows unchanged edit submit when browser clock is ahead of endsAt", async () => {
    jest.useFakeTimers();
    jest.setSystemTime(new Date("2026-01-02T14:30:00Z"));

    const stores = createStores(true);
    stores.silenceFormStore.data.setStart(new Date("2026-01-02T10:00:00Z"));
    stores.silenceFormStore.data.setEnd(new Date("2026-01-02T13:00:00Z"));

    let submitPayload: Record<string, unknown> | null = null;
    global.fetch = jest.fn(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        if (input.toString().includes("/silence-editor/preview")) {
          return previewResponse();
        }
        submitPayload = parseSubmitPayload(init);
        return new Response(JSON.stringify({ silenceID: "silence-1" }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        });
      },
    ) as typeof fetch;

    renderEditor(stores);

    await act(async () => {
      fireEvent.click(screen.getByRole("button", { name: "Update silence" }));
    });

    expect(submitPayload).toMatchObject({
      id: "silence-1",
      startsAt: "2026-01-02T10:00:00.000Z",
      endsAt: "2026-01-02T13:00:00.000Z",
    });
  });

  it("keeps fixed durations as elapsed time across DST boundaries", async () => {
    const previousTZ = process.env.TZ;
    process.env.TZ = "America/New_York";
    try {
      jest.useFakeTimers();
      jest.setSystemTime(new Date("2026-03-08T06:30:00Z"));
      const stores = createStores(false);
      let submitPayload: Record<string, unknown> | null = null;

      global.fetch = jest.fn(
        async (input: RequestInfo | URL, init?: RequestInit) => {
          if (input.toString().includes("/silence-editor/preview")) {
            return previewResponse();
          }
          submitPayload = parseSubmitPayload(init);
          return new Response(JSON.stringify({ silenceID: "s-dst" }), {
            status: 200,
            headers: { "Content-Type": "application/json" },
          });
        },
      ) as typeof fetch;

      renderEditor(stores);
      fillRequiredMetadata();
      const endsSelect = screen.getAllByRole("combobox")[2];
      fireEvent.change(endsSelect, { target: { value: "preset:1d" } });

      await act(async () => {
        fireEvent.click(screen.getByRole("button", { name: "Create silence" }));
      });

      const startsAt = Date.parse(String(submitPayload?.["startsAt"] || ""));
      const endsAt = Date.parse(String(submitPayload?.["endsAt"] || ""));
      expect(endsAt - startsAt).toBe(24 * 60 * 60 * 1000);
    } finally {
      process.env.TZ = previousTZ;
    }
  });

  it("recreates an expired silence without its ID and preserves elapsed duration", async () => {
    jest.useFakeTimers();
    jest.setSystemTime(new Date("2026-01-03T12:00:00Z"));
    const stores = createStores(false);
    stores.silenceFormStore.data.setSilenceID("expired-silence");
    stores.silenceFormStore.data.setOperation("recreate");
    stores.silenceFormStore.data.setStart(new Date("2026-01-01T10:00:00Z"));
    stores.silenceFormStore.data.setEnd(new Date("2026-01-01T13:00:00Z"));
    let submitPayload: Record<string, unknown> | null = null;
    global.fetch = jest.fn(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        if (input.toString().includes("/silence-editor/preview")) {
          return previewResponse();
        }
        submitPayload = parseSubmitPayload(init);
        return new Response(JSON.stringify({ silenceID: "new-silence" }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        });
      },
    ) as typeof fetch;

    renderEditor(stores);
    expect(
      screen.getByRole("button", { name: "Recreate silence" }),
    ).toBeEnabled();

    await act(async () => {
      fireEvent.click(screen.getByRole("button", { name: "Recreate silence" }));
    });

    expect(submitPayload).not.toHaveProperty("id");
    expect(submitPayload).not.toBeNull();
    const startsAt = Date.parse(submitPayload!.startsAt as string);
    const endsAt = Date.parse(submitPayload!.endsAt as string);
    expect(startsAt).toBe(Date.now());
    expect(endsAt - startsAt).toBe(3 * 60 * 60 * 1000);
  });

  it("initializes create defaults from auth, filters, and configured Alertmanagers", () => {
    const stores = createStores(false);
    stores.silenceFormStore.data.setMatchers([]);
    stores.silenceFormStore.data.setAutofillMatchers(true);
    stores.settingsStore.silenceFormConfig.saveAuthor("stored@example.com");
    stores.alertStore.info.setAuthentication(true, "authenticated@example.com");
    stores.alertStore.settings.values.silenceForm.defaultAlertmanagers = [
      "am1",
    ];
    const filter = NewUnappliedFilter("service=api");
    filter.name = "service";
    filter.matcher = QueryOperators.Equal;
    filter.value = "api";
    stores.alertStore.filters.setFilterValues([filter]);

    renderEditor(stores);

    expect(screen.getAllByRole("combobox")[0]).toHaveValue("am1");
    expect(screen.getByDisplayValue('service="api"')).toBeInTheDocument();
    expect(
      screen.getByDisplayValue("authenticated@example.com"),
    ).toBeInTheDocument();
  });

  it("autofill wraps regex filters with grouped case-insensitive contains semantics", () => {
    const stores = createStores(false);
    stores.silenceFormStore.data.setMatchers([]);
    stores.silenceFormStore.data.setAutofillMatchers(true);
    const filter = NewUnappliedFilter("service=~api|payments");
    filter.name = "service";
    filter.matcher = QueryOperators.Regex;
    filter.value = "api|payments";
    stores.alertStore.filters.setFilterValues([filter]);

    renderEditor(stores);

    expect(
      screen.getByDisplayValue('service=~"(?i:.*(?:api|payments).*)"'),
    ).toBeInTheDocument();
  });

  it("autofill escapes literal line feeds before rendering the matcher input", () => {
    const stores = createStores(false);
    stores.silenceFormStore.data.setMatchers([]);
    stores.silenceFormStore.data.setAutofillMatchers(true);
    const filter = NewUnappliedFilter("summary=line one");
    filter.name = "summary";
    filter.matcher = QueryOperators.Equal;
    filter.value = "line one\nline two";
    stores.alertStore.filters.setFilterValues([filter]);

    renderEditor(stores);

    expect(
      screen.getByDisplayValue('summary="line one\\nline two"'),
    ).toBeInTheDocument();
  });

  it("uses the versioned stored author when authentication is disabled", () => {
    const stores = createStores(false);
    localStorage.setItem(
      "karmaSilenceEditorAuthorV1",
      "stored-editor@example.com",
    );

    renderEditor(stores);

    expect(
      screen.getByDisplayValue("stored-editor@example.com"),
    ).toBeInTheDocument();
  });

  it("uses the configured author and only logs a warning when localStorage cannot be read", () => {
    const stores = createStores(false);
    stores.settingsStore.silenceFormConfig.saveAuthor("configured@example.com");
    const storageError = new DOMException(
      "Storage is disabled",
      "SecurityError",
    );
    jest.spyOn(Storage.prototype, "getItem").mockImplementation(() => {
      throw storageError;
    });
    const warn = jest.spyOn(console, "warn").mockImplementation(() => {});

    renderEditor(stores);

    expect(
      screen.getByDisplayValue("configured@example.com"),
    ).toBeInTheDocument();
    expect(warn).toHaveBeenCalledWith(
      "Unable to read the saved silence author from localStorage.",
      storageError,
    );
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
  });

  it("uses one close guard for dirty state and blocks closing during submit", async () => {
    const stores = createStores(false);
    let guard = (_reason: SilenceEditorCloseReasonT) => true;
    let resolveSubmit: (response: Response) => void = () => {};
    global.fetch = jest.fn((input: RequestInfo | URL) => {
      if (input.toString().includes("/silence-editor/preview")) {
        return Promise.resolve(previewResponse());
      }
      return new Promise<Response>((resolve) => {
        resolveSubmit = resolve;
      });
    }) as typeof fetch;
    const confirmSpy = jest.spyOn(window, "confirm").mockReturnValue(false);
    renderEditor(stores, {
      registerCloseGuard: (nextGuard) => {
        guard = nextGuard;
      },
    });

    const commentField = screen
      .getByText("Comment")
      .parentElement?.querySelector("textarea");
    fireEvent.change(commentField as HTMLTextAreaElement, {
      target: { value: "changed comment" },
    });
    expect(guard("close")).toBe(false);
    expect(guard("replace")).toBe(false);
    expect(confirmSpy).toHaveBeenCalledWith("Discard unsaved silence changes?");

    confirmSpy.mockReturnValue(true);
    expect(guard("close")).toBe(true);
    fillRequiredMetadata("me@example.com", "changed comment");
    fireEvent.click(screen.getByRole("button", { name: "Create silence" }));
    await waitFor(() => expect(guard("close")).toBe(false));
    expect(guard("replace")).toBe(false);

    await act(async () => {
      resolveSubmit(
        new Response(JSON.stringify({ silenceID: "s-guard" }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        }),
      );
    });
  });

  it("does not mark matcher-only formatting changes as dirty", () => {
    const stores = createStores(false);
    let guard = (_reason: SilenceEditorCloseReasonT) => true;
    global.fetch = jest.fn(() =>
      Promise.resolve(previewResponse()),
    ) as typeof fetch;
    const confirmSpy = jest.spyOn(window, "confirm").mockReturnValue(false);
    const { container } = renderEditor(stores, {
      registerCloseGuard: (nextGuard) => {
        guard = nextGuard;
      },
    });
    const matcher = container.querySelector<HTMLInputElement>(
      ".fork-silence-editor__matcher-expression",
    );
    if (!matcher) {
      throw new Error("matcher input not rendered");
    }
    const original = matcher.value;
    fireEvent.focus(matcher);
    fireEvent.change(matcher, {
      target: { value: original.replace(/(=~|!~|!=|=)/, " $1 ") },
    });

    expect(guard("close")).toBe(true);
    expect(confirmSpy).not.toHaveBeenCalled();
  });

  it("ignores stale submit completion after unmount and never calls old onClose", async () => {
    const stores = createStores(false);

    let resolveSubmit: ((response: Response) => void) | null = null;
    global.fetch = jest.fn((input: RequestInfo | URL) => {
      if (input.toString().includes("/silence-editor/preview")) {
        return Promise.resolve(previewResponse());
      }
      return new Promise<Response>((resolve) => {
        resolveSubmit = resolve;
      });
    }) as typeof fetch;

    const oldOnClose = jest.fn();
    const oldOnSubmitted = jest.fn();
    const first = renderEditor(stores, {
      onClose: oldOnClose,
      onSubmitted: oldOnSubmitted,
    });

    fillRequiredMetadata();
    fireEvent.click(screen.getByRole("button", { name: "Create silence" }));
    await waitFor(() => {
      expect(resolveSubmit).not.toBeNull();
    });

    first.unmount();

    renderEditor(createStores(false));

    await act(async () => {
      resolveSubmit!(
        new Response(JSON.stringify({ silenceID: "old-editor" }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        }),
      );
    });

    expect(oldOnSubmitted).not.toHaveBeenCalled();
    expect(oldOnClose).not.toHaveBeenCalled();
  });

  it.each([
    ["proxy", "unmount"],
    ["proxy", "replacement"],
    ["direct", "unmount"],
    ["direct", "replacement"],
  ] as const)(
    "aborts %s submit on %s without invoking stale callbacks",
    async (mode, owner) => {
      const stores = createStores(false);
      const info = jest.spyOn(console, "info").mockImplementation(() => {});
      const error = jest.spyOn(console, "error").mockImplementation(() => {});
      let mutationSignal: AbortSignal | null = null;
      const oldOnClose = jest.fn();
      const oldOnSubmitted = jest.fn();
      global.fetch = jest.fn((input: RequestInfo | URL, init?: RequestInit) => {
        const url = input.toString();
        if (url.includes("/silence-editor/preview")) {
          return Promise.resolve(previewResponse());
        }
        if (url.includes("/silence-editor/submit") && mode === "direct") {
          const submitted = JSON.parse((init?.body as string) || "{}") as {
            payload: AlertmanagerSilencePayloadT;
          };
          return Promise.resolve(
            new Response(
              JSON.stringify({
                mode: "direct",
                correlationID: "direct-correlation-owner-sequence",
                direct: {
                  endpoint: "http://am1.example.com/api/v2/silences",
                  member: "am1",
                  corsCredentials: "same-origin",
                  payload: submitted.payload,
                },
              }),
              {
                status: 200,
                headers: { "Content-Type": "application/json" },
              },
            ),
          );
        }
        if (
          (mode === "proxy" && url.includes("/silence-editor/submit")) ||
          (mode === "direct" &&
            url === "http://am1.example.com/api/v2/silences")
        ) {
          mutationSignal = init?.signal as AbortSignal;
          return new Promise<Response>((_resolve, reject) => {
            if (mode === "direct") {
              init?.signal?.addEventListener("abort", () => {
                reject(new DOMException("Aborted", "AbortError"));
              });
            }
          });
        }
        throw new Error(`unexpected request ${url}`);
      }) as typeof fetch;
      const rendered = renderEditor(stores, {
        onClose: oldOnClose,
        onSubmitted: oldOnSubmitted,
      });
      fillRequiredMetadata();
      fireEvent.click(screen.getByRole("button", { name: "Create silence" }));
      await waitFor(() => expect(mutationSignal).not.toBeNull());
      const capturedSignal = mutationSignal as AbortSignal | null;
      rendered.unmount();
      if (owner === "replacement") {
        renderEditor(createStores(false));
      }
      expect(capturedSignal?.aborted).toBe(true);
      expect(oldOnSubmitted).not.toHaveBeenCalled();
      expect(oldOnClose).not.toHaveBeenCalled();
      const expectedAuditCalls = Number(mode === "direct");
      await waitFor(() => {
        expect(info).toHaveBeenCalledTimes(expectedAuditCalls);
        expect(error).toHaveBeenCalledTimes(expectedAuditCalls);
      });
    },
  );

  it("computes Now and duration from one submit-time timestamp", async () => {
    jest.useFakeTimers();
    jest.setSystemTime(new Date("2026-01-02T12:00:00Z"));
    const stores = createStores(false);
    let submitPayload: Record<string, unknown> | null = null;
    global.fetch = jest.fn(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        if (input.toString().includes("/silence-editor/preview")) {
          return previewResponse();
        }
        submitPayload = parseSubmitPayload(init);
        return new Response(JSON.stringify({ silenceID: "s-now" }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        });
      },
    ) as typeof fetch;
    renderEditor(stores);
    fillRequiredMetadata();

    act(() => {
      jest.advanceTimersByTime(45 * 60 * 1000);
    });
    await act(async () => {
      fireEvent.click(screen.getByRole("button", { name: "Create silence" }));
    });

    expect(Date.parse(String(submitPayload?.["startsAt"]))).toBe(Date.now());
    expect(
      Date.parse(String(submitPayload?.["endsAt"])) -
        Date.parse(String(submitPayload?.["startsAt"])),
    ).toBe(60 * 60 * 1000);
  });

  it("loads subsequent preview pages using the backend cursor", async () => {
    const stores = createStores(false);
    const cursors: Array<string | undefined> = [];
    global.fetch = jest.fn(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        if (!input.toString().includes("/silence-editor/preview")) {
          return new Response(JSON.stringify({ silenceID: "unused" }), {
            status: 200,
          });
        }
        const request = JSON.parse((init?.body as string) || "{}");
        cursors.push(request.cursor);
        return request.cursor
          ? previewResponse([previewAlert("alert-2")], undefined, 2)
          : previewResponse([previewAlert("alert-1")], "cursor-2", 2);
      },
    ) as typeof fetch;

    renderEditor(stores);
    await waitFor(() =>
      expect(screen.getByRole("button", { name: "Load more" })).toBeEnabled(),
    );
    fireEvent.click(screen.getByRole("button", { name: "Load more" }));

    await waitFor(() =>
      expect(
        screen.getByText(
          (_, element) => element?.textContent === "alertname=alert-2",
        ),
      ).toBeInTheDocument(),
    );
    expect(cursors).toEqual([undefined, "cursor-2"]);
  });

  it("keeps submission disabled after a successful response", async () => {
    const stores = createStores(false);
    const onSubmitted = jest.fn();
    let submissions = 0;
    global.fetch = jest.fn(async (input: RequestInfo | URL) => {
      if (input.toString().includes("/silence-editor/preview")) {
        return previewResponse();
      }
      submissions++;
      return new Response(JSON.stringify({ silenceID: "s-success" }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    }) as typeof fetch;

    renderEditor(stores, { onSubmitted });
    const submit = screen.getByRole("button", { name: "Create silence" });
    fillRequiredMetadata();
    fireEvent.click(submit);
    await waitFor(() =>
      expect(screen.getByText(/s-success/)).toBeInTheDocument(),
    );

    expect(submit).toBeDisabled();
    fireEvent.click(submit);
    expect(submissions).toBe(1);
    expect(onSubmitted).toHaveBeenCalledWith("am1", "s-success");
    expect(localStorage.getItem("karmaSilenceEditorAuthorV1")).toBe(
      "me@example.com",
    );
  });

  it("keeps a successful submission successful when localStorage cannot save the author", async () => {
    const stores = createStores(false);
    const onSubmitted = jest.fn();
    const storageError = new DOMException(
      "Storage is disabled",
      "SecurityError",
    );
    jest.spyOn(Storage.prototype, "setItem").mockImplementation(() => {
      throw storageError;
    });
    const warn = jest.spyOn(console, "warn").mockImplementation(() => {});
    global.fetch = jest.fn(async (input: RequestInfo | URL) => {
      if (input.toString().includes("/silence-editor/preview")) {
        return previewResponse();
      }
      return new Response(JSON.stringify({ silenceID: "s-storage-disabled" }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    }) as typeof fetch;

    renderEditor(stores, { onSubmitted });
    fillRequiredMetadata();
    fireEvent.click(screen.getByRole("button", { name: "Create silence" }));

    await waitFor(() =>
      expect(screen.getByText(/s-storage-disabled/)).toBeInTheDocument(),
    );
    expect(onSubmitted).toHaveBeenCalledWith("am1", "s-storage-disabled");
    expect(warn).toHaveBeenCalledWith(
      "Unable to save the silence author to localStorage.",
      storageError,
    );
  });

  it("distinguishes matcher resting/editing states and Escape reverts edits", () => {
    const stores = createStores(false);
    renderEditor(stores);
    const resting = screen.getByLabelText("Matcher 1 expression, resting");
    expect(resting).toHaveValue('alertname="Host_Down"');

    fireEvent.focus(resting);
    const editing = screen.getByLabelText("Matcher 1 expression, editing");
    fireEvent.change(editing, { target: { value: 'alertname="Changed"' } });
    fireEvent.keyDown(editing, { key: "Escape" });

    expect(screen.getByLabelText("Matcher 1 expression, resting")).toHaveValue(
      'alertname="Host_Down"',
    );
  });

  it("keeps edited matcher backup aligned when removing an earlier matcher", () => {
    const stores = createStores(false);
    stores.silenceFormStore.data.setMatchers([
      {
        id: "m1",
        name: "first",
        values: [StringToOption("one")],
        isRegex: false,
        isEqual: true,
      },
      {
        id: "m2",
        name: "second",
        values: [StringToOption("two")],
        isRegex: false,
        isEqual: true,
      },
      {
        id: "m3",
        name: "third",
        values: [StringToOption("three")],
        isRegex: false,
        isEqual: true,
      },
    ]);

    renderEditor(stores);

    const thirdResting = screen.getByLabelText("Matcher 3 expression, resting");
    fireEvent.focus(thirdResting);
    const thirdEditing = screen.getByLabelText("Matcher 3 expression, editing");
    fireEvent.change(thirdEditing, {
      target: { value: 'third="changed"' },
    });

    fireEvent.click(screen.getByRole("button", { name: "Remove matcher 1" }));

    const nowSecondEditing = screen.getByLabelText(
      "Matcher 2 expression, editing",
    );
    fireEvent.keyDown(nowSecondEditing, { key: "Escape" });

    expect(screen.getByLabelText("Matcher 1 expression, resting")).toHaveValue(
      'second="two"',
    );
    expect(screen.getByLabelText("Matcher 2 expression, resting")).toHaveValue(
      'third="three"',
    );
  });

  it("renders regexp operators and non-literal tokens with semantic classes", () => {
    const stores = createStores(false);
    const matcher = stores.silenceFormStore.data.matchers[0];
    matcher.isRegex = true;
    matcher.values = [
      { ...StringToOption("api|payments.*"), wasCreated: true },
    ];

    const { container } = renderEditor(stores);

    expect(
      container.querySelector(".syntax-operator.regexp"),
    ).toHaveTextContent("=~");
    expect(
      Array.from(container.querySelectorAll(".syntax-regexp-meta")).map(
        (node) => node.textContent,
      ),
    ).toEqual(expect.arrayContaining(["|", ".", "*"]));
  });

  it("supports keyboard navigation in label autocomplete", async () => {
    jest.useFakeTimers();
    const stores = createStores(false);
    global.fetch = jest.fn(async (input: RequestInfo | URL) => {
      const url = input.toString();
      if (url.includes("/silence-editor/labels")) {
        return new Response(
          JSON.stringify({
            status: "success",
            target: "am1",
            total: 2000,
            truncated: true,
            labels: [
              { name: "service", values: 3 },
              { name: "team", values: 2 },
            ],
          }),
          { status: 200, headers: { "Content-Type": "application/json" } },
        );
      }
      return previewResponse();
    }) as typeof fetch;
    renderEditor(stores);
    const input = screen.getByLabelText("Matcher 1 expression, resting");
    fireEvent.focus(input);
    fireEvent.change(input, { target: { value: "s" } });
    fireEvent.keyUp(input, { key: "s" });
    await act(async () => {
      jest.advanceTimersByTime(220);
    });

    await waitFor(() =>
      expect(
        screen.getByRole("option", { name: /service/ }),
      ).toBeInTheDocument(),
    );
    expect(
      screen.getByText("Showing 2 of 2000 label names"),
    ).toBeInTheDocument();
    const listbox = screen.getByRole("listbox");
    const serviceOption = screen.getByRole("option", { name: /service/ });
    expect(input).toHaveAttribute("role", "combobox");
    expect(input).toHaveAttribute("aria-controls", listbox.id);
    expect(input).toHaveAttribute("aria-activedescendant", serviceOption.id);
    fireEvent.keyDown(input, { key: "ArrowDown" });
    await waitFor(() =>
      expect(input).toHaveAttribute(
        "aria-activedescendant",
        screen.getByRole("option", { name: /team/ }).id,
      ),
    );
    fireEvent.mouseEnter(screen.getByRole("option", { name: /service/ }));
    expect(input).toHaveAttribute(
      "aria-activedescendant",
      serviceOption.id,
    );
    expect(serviceOption).toHaveAttribute("aria-selected", "true");
    fireEvent.keyDown(input, { key: "Enter" });

    expect(input).toHaveValue('service=""');
  });

  it("sends only completed sibling matchers as autocomplete context", async () => {
    jest.useFakeTimers();
    const stores = createStores(false);
    const editingMatcher = NewEmptyMatcher();
    stores.silenceFormStore.data.setMatchers([
      stores.silenceFormStore.data.matchers[0],
      editingMatcher,
    ]);
    let requestBody: Record<string, unknown> | undefined;
    global.fetch = jest.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      if (input.toString().includes("/silence-editor/labels")) {
        requestBody = JSON.parse(init?.body as string) as Record<string, unknown>;
        return new Response(
          JSON.stringify({
            status: "success",
            target: "am1",
            contextFingerprint: "context-1",
            total: 1,
            truncated: false,
            labels: [{ name: "service", values: 1 }],
          }),
          { status: 200, headers: { "Content-Type": "application/json" } },
        );
      }
      return previewResponse();
    }) as typeof fetch;

    renderEditor(stores);
    const input = screen.getByLabelText("Matcher 2 expression, resting");
    fireEvent.focus(input);
    fireEvent.change(input, { target: { value: "s" } });
    fireEvent.keyUp(input, { key: "s" });
    await act(async () => {
      jest.advanceTimersByTime(220);
      await Promise.resolve();
    });

    await waitFor(() =>
      expect(requestBody?.contextMatchers).toEqual([
        { name: "alertname", value: "Host_Down", isRegex: false, isEqual: true },
      ]),
    );
    expect(requestBody?.existingMatcherNames).toEqual(["alertname"]);
  });

  it("keeps autocomplete open while interacting with its scrollbar", async () => {
    jest.useFakeTimers();
    const stores = createStores(false);
    global.fetch = jest.fn(async (input: RequestInfo | URL) => {
      if (input.toString().includes("/silence-editor/labels")) {
        return new Response(
          JSON.stringify({
            status: "success",
            target: "am1",
            total: 2,
            truncated: false,
            labels: [
              { name: "service", values: 3 },
              { name: "team", values: 2 },
            ],
          }),
          { status: 200, headers: { "Content-Type": "application/json" } },
        );
      }
      return previewResponse();
    }) as typeof fetch;
    renderEditor(stores);
    const input = screen.getByLabelText("Matcher 1 expression, resting");
    fireEvent.focus(input);
    fireEvent.change(input, { target: { value: "s" } });
    fireEvent.keyUp(input, { key: "s" });
    await act(async () => {
      jest.advanceTimersByTime(220);
    });

    const listbox = await screen.findByRole("listbox");
    fireEvent.pointerDown(listbox);
    fireEvent.blur(input);
    await act(async () => {
      jest.advanceTimersByTime(100);
    });

    expect(listbox).toBeInTheDocument();
  });

  it("keeps the keyboard-selected autocomplete option visible", async () => {
    jest.useFakeTimers();
    const stores = createStores(false);
    global.fetch = jest.fn(async (input: RequestInfo | URL) => {
      if (input.toString().includes("/silence-editor/labels")) {
        return new Response(
          JSON.stringify({
            status: "success",
            target: "am1",
            total: 8,
            truncated: false,
            labels: Array.from({ length: 8 }, (_, index) => ({
              name: `label_${index}`,
              values: index + 1,
            })),
          }),
          { status: 200, headers: { "Content-Type": "application/json" } },
        );
      }
      return previewResponse();
    }) as typeof fetch;
    renderEditor(stores);
    const input = screen.getByLabelText("Matcher 1 expression, resting");
    fireEvent.focus(input);
    fireEvent.change(input, { target: { value: "l" } });
    fireEvent.keyUp(input, { key: "l" });
    await act(async () => {
      jest.advanceTimersByTime(220);
    });

    const listbox = await screen.findByRole("listbox");
    const options = screen.getAllByRole("option");
    Object.defineProperties(listbox, {
      scrollTop: { configurable: true, value: 0, writable: true },
      getBoundingClientRect: {
        configurable: true,
        value: () => ({ top: 0, bottom: 80 }),
      },
    });
    options.forEach((option, index) => {
      Object.defineProperty(option, "getBoundingClientRect", {
        configurable: true,
        value: () => ({ top: index * 40, bottom: index * 40 + 40 }),
      });
    });

    for (let i = 0; i < 4; i++) {
      fireEvent.keyDown(input, { key: "ArrowDown" });
    }

    await waitFor(() => expect(listbox.scrollTop).toBeGreaterThan(0));
    expect(input).toHaveAttribute("aria-activedescendant", expect.any(String));
  });

  it("blocks Alertmanager submission when backend RE2 validation fails", async () => {
    const stores = createStores(false);
    let alertmanagerRequests = 0;
    global.fetch = jest.fn(async (input: RequestInfo | URL) => {
      if (input.toString().includes("/silence-editor/preview")) {
        return new Response(
          JSON.stringify({ error: "invalid regex for service" }),
          {
            status: 400,
            headers: { "Content-Type": "application/json" },
          },
        );
      }
      alertmanagerRequests++;
      return new Response(JSON.stringify({ silenceID: "must-not-exist" }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    }) as typeof fetch;
    renderEditor(stores);
    fillRequiredMetadata();
    const matcher = screen.getByLabelText("Matcher 1 expression, resting");
    fireEvent.focus(matcher);
    fireEvent.change(matcher, {
      target: { value: 'service=~"(?=api)api"' },
    });
    fireEvent.keyDown(matcher, { key: "Enter" });

    fireEvent.click(screen.getByRole("button", { name: "Create silence" }));

    await waitFor(() =>
      expect(screen.getByText("invalid regex for service")).toBeInTheDocument(),
    );
    expect(alertmanagerRequests).toBe(0);
    expect(
      screen.getByRole("button", { name: "Create silence" }),
    ).toBeDisabled();
  });

  it("accepts Alertmanager-compatible unquoted matcher values", async () => {
    const stores = createStores(false);
    let submittedPayload: Record<string, unknown> | null = null;
    global.fetch = jest.fn(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        if (input.toString().includes("/silence-editor/preview")) {
          return previewResponse();
        }
        submittedPayload = parseSubmitPayload(init);
        return new Response(JSON.stringify({ silenceID: "unquoted-ok" }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        });
      },
    ) as typeof fetch;

    renderEditor(stores);
    fillRequiredMetadata();
    const matcher = screen.getByLabelText("Matcher 1 expression, resting");
    fireEvent.focus(matcher);
    fireEvent.change(matcher, { target: { value: "severity=critical" } });
    fireEvent.keyDown(matcher, { key: "Enter" });

    const submit = screen.getByRole("button", { name: "Create silence" });
    await waitFor(() => expect(submit).toBeEnabled());
    fireEvent.click(submit);
    await waitFor(() => expect(submittedPayload).not.toBeNull());

    expect(submittedPayload?.["matchers"]).toEqual([
      { name: "severity", value: "critical", isRegex: false, isEqual: true },
    ]);
  });

  it("keeps submit disabled for unsafe matcher set until backend preflight succeeds", async () => {
    const stores = createStores(false);
    global.fetch = jest.fn(async (input: RequestInfo | URL) => {
      if (input.toString().includes("/silence-editor/preview")) {
        return new Response(
          JSON.stringify({ error: "matcher set too broad" }),
          {
            status: 400,
            headers: { "Content-Type": "application/json" },
          },
        );
      }
      return new Response(JSON.stringify({ silenceID: "must-not-exist" }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    }) as typeof fetch;

    renderEditor(stores);
    fillRequiredMetadata();

    const matcher = screen.getByLabelText("Matcher 1 expression, resting");
    fireEvent.focus(matcher);
    fireEvent.change(matcher, { target: { value: 'service=~".*"' } });
    fireEvent.keyDown(matcher, { key: "Enter" });

    await waitFor(() =>
      expect(screen.getByText("matcher set too broad")).toBeInTheDocument(),
    );
    expect(screen.getAllByText("matcher set too broad")).toHaveLength(1);
    expect(
      screen.getByRole("button", { name: "Create silence" }),
    ).toBeDisabled();
  });

  it("keeps the last successful preview visible and marks it stale", async () => {
    const stores = createStores(false);
    global.fetch = jest.fn(async () =>
      previewResponse([previewAlert("alert-before-invalid-edit")]),
    ) as typeof fetch;
    renderEditor(stores);
    await waitFor(() =>
      expect(
        screen.getByText(
          (_, element) =>
            element?.textContent === "alertname=alert-before-invalid-edit",
        ),
      ).toBeInTheDocument(),
    );
    const matcher = screen.getByLabelText("Matcher 1 expression, resting");
    fireEvent.focus(matcher);
    fireEvent.change(matcher, { target: { value: "invalid matcher" } });

    expect(screen.getByText("Stale: matcher verification failed")).toHaveClass(
      "bg-warning",
    );
    expect(
      screen.getByText(
        (_, element) =>
          element?.textContent === "alertname=alert-before-invalid-edit",
      ),
    ).toBeInTheDocument();

    fireEvent.change(matcher, { target: { value: 'alertname="Host_Down"' } });
    expect(
      screen.queryByText('Each matcher must use full expression format: label="value"'),
    ).not.toBeInTheDocument();
  });

  it("loads preview before author and comment are complete", async () => {
    const stores = createStores(false);
    stores.silenceFormStore.data.setAuthor("");
    stores.silenceFormStore.data.setComment("");
    const previewRequests: string[] = [];
    global.fetch = jest.fn(async (input: RequestInfo | URL) => {
      const url = input.toString();
      if (url.includes("/silence-editor/preview")) {
        previewRequests.push(url);
        return previewResponse([previewAlert("metadata-independent-preview")]);
      }
      return new Response(JSON.stringify({ silenceID: "unused" }), {
        status: 200,
      });
    }) as typeof fetch;

    renderEditor(stores);

    await waitFor(() => expect(previewRequests).toHaveLength(1));
    expect(
      screen.getByText(
        (_, element) =>
          element?.textContent === "alertname=metadata-independent-preview",
      ),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: "Create silence" }),
    ).toBeDisabled();
  });

  it("ignores an in-flight preview immediately after matcher input changes", async () => {
    jest.useFakeTimers();
    const stores = createStores(false);
    let resolvePreview: (response: Response) => void = () => {};
    const fetchMock = jest.fn(
      () =>
        new Promise<Response>((resolve) => {
          resolvePreview = resolve;
        }),
    );
    global.fetch = fetchMock as typeof fetch;

    renderEditor(stores);
    await act(async () => {
      jest.advanceTimersByTime(350);
    });
    expect(fetchMock.mock.calls).toHaveLength(1);

    fireEvent.change(screen.getByLabelText("Matcher 1 expression, resting"), {
      target: { value: "invalid matcher" },
    });
    await act(async () => {
      resolvePreview(
        previewResponse([previewAlert("obsolete-preview-result")]),
      );
      await Promise.resolve();
    });

    expect(
      screen.getAllByText(/Each matcher must use full expression format/),
    ).not.toHaveLength(0);
    expect(
      screen.queryByText(/obsolete-preview-result/),
    ).not.toBeInTheDocument();
  });

  it("accepts RE2-only matcher syntax and delegates validation to backend", async () => {
    const stores = createStores(false);
    let previewRequests = 0;
    global.fetch = jest.fn(async (input: RequestInfo | URL) => {
      if (input.toString().includes("/silence-editor/preview")) {
        previewRequests++;
        return previewResponse();
      }
      return new Response(JSON.stringify({ silenceID: "unused" }), {
        status: 200,
      });
    }) as typeof fetch;

    renderEditor(stores);
    const matcher = screen.getByLabelText("Matcher 1 expression, resting");
    fireEvent.focus(matcher);
    fireEvent.change(matcher, {
      target: { value: 'service=~"(?P<name>api)"' },
    });

    await waitFor(() => expect(previewRequests).toBeGreaterThan(0));
    expect(
      screen.queryByText(/Each matcher must use full expression format/),
    ).not.toBeInTheDocument();
  });

  it("ignores out-of-order autocomplete responses", async () => {
    jest.useFakeTimers();
    const stores = createStores(false);
    let firstResolve: (response: Response) => void = () => {};
    let secondResolve: (response: Response) => void = () => {};
    let labelCalls = 0;
    global.fetch = jest.fn((input: RequestInfo | URL) => {
      const url = input.toString();
      if (url.includes("/silence-editor/labels")) {
        labelCalls++;
        return new Promise<Response>((resolve) => {
          if (labelCalls === 1) {
            firstResolve = resolve;
          } else {
            secondResolve = resolve;
          }
        });
      }
      return Promise.resolve(previewResponse());
    }) as typeof fetch;

    renderEditor(stores);
    const input = screen.getByLabelText("Matcher 1 expression, resting");
    fireEvent.focus(input);
    fireEvent.change(input, { target: { value: "s" } });
    fireEvent.keyUp(input, { key: "s" });
    await act(async () => {
      jest.advanceTimersByTime(220);
    });
    fireEvent.change(input, { target: { value: "se" } });
    fireEvent.keyUp(input, { key: "e" });
    await act(async () => {
      jest.advanceTimersByTime(220);
    });

    await act(async () => {
      secondResolve(
        new Response(
          JSON.stringify({
            status: "success",
            target: "am1",
            total: 1,
            truncated: false,
            labels: [{ name: "service", values: 3 }],
          }),
          { status: 200, headers: { "Content-Type": "application/json" } },
        ),
      );
      await Promise.resolve();
    });
    await waitFor(() =>
      expect(
        screen.getByRole("option", { name: /service/ }),
      ).toBeInTheDocument(),
    );

    await act(async () => {
      firstResolve(
        new Response(
          JSON.stringify({
            status: "success",
            target: "am1",
            total: 1,
            truncated: false,
            labels: [{ name: "severity", values: 9 }],
          }),
          { status: 200, headers: { "Content-Type": "application/json" } },
        ),
      );
      await Promise.resolve();
    });

    expect(screen.queryByRole("option", { name: /severity/ })).toBeNull();
    expect(screen.getByRole("option", { name: /service/ })).toBeInTheDocument();
  });

  it("debounces autocomplete requests and aborts in-flight older request", async () => {
    jest.useFakeTimers();
    const stores = createStores(false);
    let calls = 0;
    const abortedSignals: AbortSignal[] = [];
    let firstResolve: ((value: Response) => void) | null = null;

    global.fetch = jest.fn((input: RequestInfo | URL, init?: RequestInit) => {
      const url = input.toString();
      if (!url.includes("/silence-editor/labels")) {
        return Promise.resolve(previewResponse());
      }
      calls++;
      const signal = init?.signal as AbortSignal | undefined;
      if (calls === 1) {
        return new Promise<Response>((resolve) => {
          firstResolve = resolve;
          if (signal) {
            signal.addEventListener("abort", () => abortedSignals.push(signal));
          }
        });
      }
      return Promise.resolve(
        new Response(
          JSON.stringify({
            status: "success",
            target: "am1",
            total: 1,
            truncated: false,
            labels: [{ name: "service", values: 1 }],
          }),
          { status: 200, headers: { "Content-Type": "application/json" } },
        ),
      );
    }) as typeof fetch;

    renderEditor(stores);
    const input = screen.getByLabelText("Matcher 1 expression, resting");
    fireEvent.focus(input);
    fireEvent.change(input, { target: { value: "s" } });
    fireEvent.keyUp(input, { key: "s" });

    await act(async () => {
      jest.advanceTimersByTime(220);
    });
    expect(calls).toBe(1);

    fireEvent.change(input, { target: { value: "se" } });
    fireEvent.keyUp(input, { key: "e" });

    await act(async () => {
      jest.advanceTimersByTime(220);
      await Promise.resolve();
    });

    expect(calls).toBe(2);
    expect(abortedSignals).toHaveLength(1);

    if (firstResolve) {
      await act(async () => {
        firstResolve!(
          new Response(
            JSON.stringify({
              status: "success",
              target: "am1",
              total: 1,
              truncated: false,
              labels: [{ name: "severity", values: 2 }],
            }),
            { status: 200, headers: { "Content-Type": "application/json" } },
          ),
        );
        await Promise.resolve();
      });
    }

    await waitFor(() =>
      expect(
        screen.getByRole("option", { name: /service/ }),
      ).toBeInTheDocument(),
    );
    expect(screen.queryByRole("option", { name: /severity/ })).toBeNull();
  });

  it("fetches value suggestions immediately after selecting a label", async () => {
    jest.useFakeTimers();
    const stores = createStores(false);
    let valuesCalls = 0;
    global.fetch = jest.fn((input: RequestInfo | URL) => {
      const url = input.toString();
      if (url.includes("/silence-editor/labels")) {
        return Promise.resolve(
          new Response(
            JSON.stringify({
              status: "success",
              target: "am1",
              total: 1,
              truncated: false,
              labels: [{ name: "service", values: 3 }],
            }),
            { status: 200, headers: { "Content-Type": "application/json" } },
          ),
        );
      }
      if (url.includes("/silence-editor/values")) {
        valuesCalls++;
        return Promise.resolve(
          new Response(
            JSON.stringify({
              status: "success",
              target: "am1",
              label: "service",
              total: 1,
              truncated: false,
              values: [{ value: "payments", count: 7 }],
            }),
            { status: 200, headers: { "Content-Type": "application/json" } },
          ),
        );
      }
      return Promise.resolve(previewResponse());
    }) as typeof fetch;

    renderEditor(stores);
    const input = screen.getByLabelText("Matcher 1 expression, resting");
    fireEvent.focus(input);
    fireEvent.change(input, { target: { value: "s" } });
    fireEvent.keyUp(input, { key: "s" });

    await act(async () => {
      jest.advanceTimersByTime(220);
      await Promise.resolve();
    });

    const serviceOption = await screen.findByRole("option", {
      name: /service/,
    });
    fireEvent.click(serviceOption);

    await act(async () => {
      jest.advanceTimersByTime(220);
      await Promise.resolve();
    });

    expect(valuesCalls).toBe(1);
    expect(
      await screen.findByRole("option", { name: /payments/ }),
    ).toBeInTheDocument();
  });

  it("disables stale load-more and does not issue stale cursor requests", async () => {
    jest.useFakeTimers();
    const stores = createStores(false);
    const requests: Array<{ cursor?: string; matchers?: unknown[] }> = [];
    global.fetch = jest.fn(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        if (!input.toString().includes("/silence-editor/preview")) {
          return new Response(JSON.stringify({ silenceID: "unused" }), {
            status: 200,
          });
        }
        const body = JSON.parse((init?.body as string) || "{}") as {
          cursor?: string;
          matchers?: unknown[];
        };
        requests.push(body);
        return body.cursor
          ? previewResponse([previewAlert("page-2")], undefined, 2)
          : previewResponse([previewAlert("page-1")], "cursor-2", 2);
      },
    ) as typeof fetch;

    renderEditor(stores);
    await act(async () => {
      jest.advanceTimersByTime(350);
    });
    await waitFor(() =>
      expect(
        screen.getByText(
          (_, element) => element?.textContent === "alertname=page-1",
        ),
      ).toBeInTheDocument(),
    );

    const loadMore = screen.getByRole("button", { name: "Load more" });
    expect(loadMore).toBeEnabled();

    fireEvent.change(screen.getByLabelText("Matcher 1 expression, resting"), {
      target: { value: "invalid matcher" },
    });

    await waitFor(() =>
      expect(screen.queryByRole("button", { name: "Load more" })).toBeNull(),
    );

    expect(requests).toEqual([
      {
        limit: 100,
        target: "am1",
        matchers: [
          {
            isEqual: true,
            isRegex: false,
            name: "alertname",
            value: "Host_Down",
          },
        ],
      },
    ]);
  });

  it("submits changed date-mode start after date-now-date toggles", async () => {
    const stores = createStores(true);
    let submitPayload: Record<string, unknown> | null = null;
    global.fetch = jest.fn(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        if (input.toString().includes("/silence-editor/preview")) {
          return previewResponse();
        }
        submitPayload = parseSubmitPayload(init);
        return new Response(JSON.stringify({ silenceID: "s-start-flip" }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        });
      },
    ) as typeof fetch;

    renderEditor(stores);
    const startMode = screen.getAllByRole("combobox")[1];
    fireEvent.change(screen.getAllByRole("combobox")[2], {
      target: { value: "preset:1h" },
    });
    fireEvent.change(startMode, { target: { value: "now" } });
    fireEvent.change(startMode, { target: { value: "date" } });

    fireEvent.change(screen.getAllByDisplayValue("2026-01-02")[0], {
      target: { value: "2026-01-03" },
    });
    const startTimeInput = document.querySelectorAll(
      '.fork-silence-editor__timing-row input[type="time"]',
    )[0] as HTMLInputElement;
    fireEvent.change(startTimeInput, {
      target: { value: "11:22" },
    });

    await act(async () => {
      fireEvent.click(screen.getByRole("button", { name: "Update silence" }));
    });

    if (submitPayload === null) {
      throw new Error("expected submit payload");
    }
    expect(String(submitPayload["startsAt"])).toBe(
      new Date(2026, 0, 3, 11, 22, 0, 0).toISOString(),
    );
  });

  it("rejects durations whose resulting timestamp is outside Date range", () => {
    const stores = createStores(false);
    renderEditor(stores);
    fireEvent.change(screen.getAllByRole("combobox")[2], {
      target: { value: "custom" },
    });

    expect(() =>
      fireEvent.change(screen.getByPlaceholderText("1w2d6h or 1y"), {
        target: { value: "999999999999999999999999h" },
      }),
    ).not.toThrow();
    expect(screen.getByText("Duration value is too large")).toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: "Create silence" }),
    ).toBeDisabled();
  });

  it("offers the documented four-day duration preset", () => {
    renderEditor(createStores(false));

    expect(screen.getByRole("option", { name: "In 4 days" })).toHaveValue(
      "preset:4d",
    );
  });

  it("keeps retry blocked after outcomeUnknown even if inputs change", async () => {
    const stores = createStores(false);
    let guard = (_reason: SilenceEditorCloseReasonT) => true;
    global.fetch = jest.fn(async (input: RequestInfo | URL) => {
      const url = input.toString();
      if (url.includes("/silence-editor/preview")) {
        return previewResponse();
      }
      return new Response(
        JSON.stringify({
          error: "network failure",
          outcomeUnknown: true,
        }),
        {
          status: 502,
          headers: { "Content-Type": "application/json" },
        },
      );
    }) as typeof fetch;

    renderEditor(stores, {
      registerCloseGuard: (nextGuard) => {
        guard = nextGuard;
      },
    });
    fillRequiredMetadata();
    const submit = screen.getByRole("button", { name: "Create silence" });
    fireEvent.click(submit);

    await waitFor(() =>
      expect(screen.getByRole("button", { name: "Retry" })).toBeDisabled(),
    );
    expect(guard("close")).toBe(true);
    fireEvent.change(screen.getByDisplayValue("test silence"), {
      target: { value: "changed comment" },
    });
    await waitFor(() =>
      expect(screen.getByRole("button", { name: "Retry" })).toBeDisabled(),
    );
  });

  it("treats proxy 2xx without silenceID as terminal unknown", async () => {
    const stores = createStores(false);
    let submitCalls = 0;
    global.fetch = jest.fn(async (input: RequestInfo | URL) => {
      const url = input.toString();
      if (url.includes("/silence-editor/preview")) return previewResponse();
      if (url.includes("/silence-editor/submit")) {
        submitCalls++;
        return new Response(JSON.stringify({ mode: "proxy" }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        });
      }
      throw new Error(`unexpected request ${url}`);
    }) as typeof fetch;

    renderEditor(stores);
    fillRequiredMetadata();
    fireEvent.click(screen.getByRole("button", { name: "Create silence" }));
    await waitFor(() =>
      expect(screen.getByRole("button", { name: "Retry" })).toBeDisabled(),
    );
    fireEvent.click(screen.getByRole("button", { name: "Retry" }));
    expect(submitCalls).toBe(1);
  });

  it("treats plain-text 401 as known failure without outcomeUnknown", async () => {
    const stores = createStores(false);
    const initialLastUpdate = stores.alertStore.status.lastUpdateAt;
    global.fetch = jest.fn(async (input: RequestInfo | URL) => {
      const url = input.toString();
      if (url.includes("/silence-editor/preview")) {
        return previewResponse();
      }
      if (url.includes("/silence-editor/submit")) {
        return new Response("session expired", {
          status: 401,
          headers: { "Content-Type": "text/plain" },
        });
      }
      return new Response(JSON.stringify({ error: "unexpected request" }), {
        status: 500,
      });
    }) as typeof fetch;

    renderEditor(stores);
    fillRequiredMetadata();
    fireEvent.click(screen.getByRole("button", { name: "Create silence" }));

    await waitFor(() =>
      expect(screen.getByText("session expired")).toBeInTheDocument(),
    );
    expect(stores.alertStore.status.lastUpdateAt).toBe(initialLastUpdate);
    expect(screen.getByRole("button", { name: "Retry" })).toBeEnabled();
    expect(screen.queryByText(/Outcome is unknown/i)).toBeNull();
  });

  it("blocks retry when backend requires reopening a stale edit operation", async () => {
    const stores = createStores(false);
    global.fetch = jest.fn(async (input: RequestInfo | URL) => {
      const url = input.toString();
      if (url.includes("/silence-editor/preview")) {
        return previewResponse();
      }
      return new Response(
        JSON.stringify({
          error:
            "silence expired while editing; reopen it as a recreate operation",
          requiresReopen: true,
        }),
        { status: 409, headers: { "Content-Type": "application/json" } },
      );
    }) as typeof fetch;

    renderEditor(stores);
    fillRequiredMetadata();
    fireEvent.click(screen.getByRole("button", { name: "Create silence" }));

    await waitFor(() =>
      expect(screen.getByRole("button", { name: "Retry" })).toBeDisabled(),
    );
    expect(
      screen.getByText(/reopen it as a recreate operation/i),
    ).toBeInTheDocument();
  });

  it("handles direct submit descriptor and performs browser-side mutation", async () => {
    const stores = createStores(false);
    const requests: string[] = [];
    let directCredentials: RequestCredentials | undefined;
    let directCreatedBy: string | null = null;
    const info = jest.spyOn(console, "info").mockImplementation(() => {});
    global.fetch = jest.fn(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = input.toString();
        requests.push(url);
        if (url.includes("/silence-editor/preview")) {
          return previewResponse();
        }
        if (url.includes("/silence-editor/submit")) {
          const submitted = JSON.parse((init?.body as string) || "{}") as {
            payload: AlertmanagerSilencePayloadT;
          };
          return new Response(
            JSON.stringify({
              status: "direct",
              target: "am1",
              mode: "direct",
              correlationID: "direct-correlation-success",
              submittedBy: "operator",
              direct: {
                endpoint: "http://am1.example.com/api/v2/silences",
                headers: { "X-Test": "1" },
                corsCredentials: "include",
                member: "am1",
                payload: {
                  ...submitted.payload,
                  createdBy: "backend-normalized@example.com",
                },
              },
            }),
            { status: 200, headers: { "Content-Type": "application/json" } },
          );
        }
        if (url === "http://am1.example.com/api/v2/silences") {
          directCredentials = init?.credentials;
          directCreatedBy = (
            JSON.parse(
              (init?.body as string) || "null",
            ) as AlertmanagerSilencePayloadT
          ).createdBy;
          return new Response(JSON.stringify({ silenceID: "direct-123" }), {
            status: 200,
            headers: { "Content-Type": "application/json" },
          });
        }
        return new Response(JSON.stringify({ error: "unexpected request" }), {
          status: 500,
        });
      },
    ) as typeof fetch;

    renderEditor(stores);
    fillRequiredMetadata();

    await act(async () => {
      fireEvent.click(screen.getByRole("button", { name: "Create silence" }));
    });

    await waitFor(() =>
      expect(
        screen.getAllByText((_, el) =>
          (el?.textContent || "").includes("direct-123"),
        ).length,
      ).toBeGreaterThan(0),
    );
    expect(requests.some((u) => u.includes("/silence-editor/submit"))).toBe(
      true,
    );
    expect(requests.includes("http://am1.example.com/api/v2/silences")).toBe(
      true,
    );
    expect(directCredentials).toBe("include");
    expect(directCreatedBy).toBe("backend-normalized@example.com");
    expect(info).toHaveBeenCalledWith("Karma silence editor", {
      component: "silence_editor",
      event: "upstream_write_started",
      correlation_id: "direct-correlation-success",
      operation: "create",
      target: "am1",
      member: "am1",
      mode: "browser_routed",
    });
    expect(info).toHaveBeenCalledWith(
      "Karma silence editor",
      expect.objectContaining({
        component: "silence_editor",
        event: "upstream_write_succeeded",
        correlation_id: "direct-correlation-success",
        operation: "create",
        target: "am1",
        member: "am1",
        mode: "browser_routed",
        http_status: 200,
        silence_id: "direct-123",
      }),
    );
    expect(JSON.stringify(info.mock.calls)).not.toContain("test silence");
    expect(JSON.stringify(info.mock.calls)).not.toContain("X-Test");
    expect(JSON.stringify(info.mock.calls)).not.toContain("Host_Down");
  });

  it("allows retry after a known direct rejection", async () => {
    const stores = createStores(false);
    jest.spyOn(console, "info").mockImplementation(() => {});
    const warn = jest.spyOn(console, "warn").mockImplementation(() => {});
    global.fetch = jest.fn(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = input.toString();
        if (url.includes("/silence-editor/preview")) return previewResponse();
        if (url.includes("/silence-editor/submit")) {
          const request = JSON.parse((init?.body as string) || "{}") as {
            payload: AlertmanagerSilencePayloadT;
          };
          return new Response(
            JSON.stringify({
              mode: "direct",
              correlationID: "direct-correlation-rejected",
              direct: {
                endpoint: "http://am1.example.com/api/v2/silences",
                member: "am1",
                corsCredentials: "same-origin",
                payload: request.payload,
              },
            }),
            { status: 200, headers: { "Content-Type": "application/json" } },
          );
        }
        return new Response("sensitive upstream rejection body", {
          status: 400,
        });
      },
    ) as typeof fetch;
    renderEditor(stores);
    fillRequiredMetadata();
    fireEvent.click(screen.getByRole("button", { name: "Create silence" }));
    await waitFor(() =>
      expect(
        screen.getByText("sensitive upstream rejection body"),
      ).toBeInTheDocument(),
    );
    expect(screen.getByRole("button", { name: "Retry" })).toBeEnabled();
    expect(warn).toHaveBeenCalledWith(
      "Karma silence editor",
      expect.objectContaining({
        component: "silence_editor",
        event: "upstream_write_rejected",
        correlation_id: "direct-correlation-rejected",
        operation: "create",
        target: "am1",
        member: "am1",
        mode: "browser_routed",
        http_status: 400,
        reason_code: "upstream_rejected",
      }),
    );
    expect(JSON.stringify(warn.mock.calls)).not.toContain(
      "sensitive upstream rejection body",
    );
  });

  it("marks outcome unknown when direct submit response cannot be parsed", async () => {
    const stores = createStores(false);
    jest.spyOn(console, "info").mockImplementation(() => {});
    const error = jest.spyOn(console, "error").mockImplementation(() => {});
    global.fetch = jest.fn(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = input.toString();
        if (url.includes("/silence-editor/preview")) {
          return previewResponse();
        }
        if (url.includes("/silence-editor/submit")) {
          const submitted = JSON.parse((init?.body as string) || "{}") as {
            payload: AlertmanagerSilencePayloadT;
          };
          return new Response(
            JSON.stringify({
              status: "direct",
              target: "am1",
              mode: "direct",
              correlationID: "direct-correlation-unknown",
              submittedBy: "operator",
              direct: {
                endpoint: "http://am1.example.com/api/v2/silences",
                headers: {},
                corsCredentials: "include",
                member: "am1",
                payload: submitted.payload,
              },
            }),
            { status: 200, headers: { "Content-Type": "application/json" } },
          );
        }
        if (url === "http://am1.example.com/api/v2/silences") {
          return new Response("{not-json", {
            status: 200,
            headers: { "Content-Type": "application/json" },
          });
        }
        return new Response(JSON.stringify({ error: "unexpected request" }), {
          status: 500,
        });
      },
    ) as typeof fetch;

    renderEditor(stores);
    fillRequiredMetadata();

    await act(async () => {
      fireEvent.click(screen.getByRole("button", { name: "Create silence" }));
    });

    await waitFor(() =>
      expect(screen.getByText(/Outcome is unknown/i)).toBeInTheDocument(),
    );
    expect(screen.getByRole("button", { name: "Retry" })).toBeDisabled();
    expect(error).toHaveBeenCalledWith(
      "Karma silence editor",
      expect.objectContaining({
        component: "silence_editor",
        event: "upstream_write_outcome_unknown",
        correlation_id: "direct-correlation-unknown",
        operation: "create",
        target: "am1",
        member: "am1",
        mode: "browser_routed",
        http_status: 200,
        failure_stage: "decode_body",
      }),
    );
    expect(JSON.stringify(error.mock.calls)).not.toContain("{not-json");
  });

  it("times out a stalled direct submit into terminal outcome unknown", async () => {
    jest.useFakeTimers();
    const stores = createStores(false);
    jest.spyOn(console, "info").mockImplementation(() => {});
    const error = jest.spyOn(console, "error").mockImplementation(() => {});
    let directStarted = false;
    global.fetch = jest.fn(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = input.toString();
        if (url.includes("/silence-editor/preview")) {
          return previewResponse();
        }
        if (url.includes("/silence-editor/submit")) {
          const submitted = JSON.parse((init?.body as string) || "{}") as {
            payload: AlertmanagerSilencePayloadT;
          };
          return new Response(
            JSON.stringify({
              status: "direct",
              target: "am1",
              mode: "direct",
              correlationID: "direct-correlation-timeout",
              submittedBy: "operator",
              direct: {
                endpoint: "http://am1.example.com/api/v2/silences",
                headers: {},
                corsCredentials: "include",
                member: "am1",
                payload: submitted.payload,
              },
            }),
            { status: 200, headers: { "Content-Type": "application/json" } },
          );
        }
        if (url === "http://am1.example.com/api/v2/silences") {
          directStarted = true;
          return await new Promise<Response>((_resolve, reject) => {
            init?.signal?.addEventListener("abort", () => {
              reject(new DOMException("Aborted", "AbortError"));
            });
          });
        }
        return new Response(JSON.stringify({ error: "unexpected request" }), {
          status: 500,
        });
      },
    ) as typeof fetch;

    renderEditor(stores);
    fillRequiredMetadata();
    await act(async () => {
      jest.advanceTimersByTime(350);
      await Promise.resolve();
    });
    await act(async () => {
      fireEvent.click(screen.getByRole("button", { name: "Create silence" }));
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(directStarted).toBe(true);

    await act(async () => {
      jest.advanceTimersByTime(30_000);
      await Promise.resolve();
    });

    expect(screen.getByText(/timed out\. Outcome is unknown/i)).toBeVisible();
    expect(screen.getByRole("button", { name: "Retry" })).toBeDisabled();
    expect(error).toHaveBeenCalledWith(
      "Karma silence editor",
      expect.objectContaining({
        event: "upstream_write_outcome_unknown",
        failure_stage: "timeout",
      }),
    );
  });

  it("resets create metadata and does not leak comment from prior edit", () => {
    const stores = createStores(false);
    stores.silenceFormStore.data.setOperation("create");
    stores.silenceFormStore.data.setAuthor("edited@example.com");
    stores.silenceFormStore.data.setComment("edited comment");
    stores.settingsStore.silenceFormConfig.saveAuthor("default@example.com");

    renderEditor(stores);

    expect(screen.getByDisplayValue("default@example.com")).toBeInTheDocument();
    const commentField = screen
      .getByText("Comment")
      .parentElement?.querySelector("textarea") as HTMLTextAreaElement;
    expect(commentField.value).toBe("");
  });

  it("preserves precise edit endsAt when absolute mode is toggled away and back", async () => {
    jest.useFakeTimers();
    jest.setSystemTime(new Date("2026-01-02T12:00:00Z"));
    const stores = createStores(true);
    stores.silenceFormStore.data.setEnd(new Date("2026-01-02T13:45:59.321Z"));
    let submitPayload: Record<string, unknown> | null = null;
    global.fetch = jest.fn(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        if (input.toString().includes("/silence-editor/preview")) {
          return previewResponse();
        }
        submitPayload = parseSubmitPayload(init);
        return new Response(JSON.stringify({ silenceID: "s-abs-toggle" }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        });
      },
    ) as typeof fetch;

    renderEditor(stores);
    const endsSelect = screen.getAllByRole("combobox")[2];
    fireEvent.change(endsSelect, { target: { value: "preset:1h" } });
    fireEvent.change(endsSelect, { target: { value: "absolute" } });

    await act(async () => {
      fireEvent.click(screen.getByRole("button", { name: "Update silence" }));
    });

    if (submitPayload === null) {
      throw new Error("expected submit payload");
    }
    expect(String(submitPayload["endsAt"])).toBe("2026-01-02T13:45:59.321Z");
  });

  it("shows matched labels that are absent from rendered labels", async () => {
    const stores = createStores(false);
    global.fetch = jest.fn(async (input: RequestInfo | URL) => {
      if (!input.toString().includes("/silence-editor/preview")) {
        return new Response(JSON.stringify({ silenceID: "unused" }), {
          status: 200,
        });
      }
      return previewResponse([
        {
          ...previewAlert("alert-with-stripped-label"),
          labels: [{ name: "alertname", value: "alert-with-stripped-label" }],
          matchedLabels: [
            { name: "alertname", value: "alert-with-stripped-label" },
            { name: "namespace", value: "prod" },
          ],
          matchedLabelNames: ["alertname", "namespace"],
        },
      ]);
    }) as typeof fetch;

    renderEditor(stores);

    await waitFor(() =>
      expect(
        screen.getByText(
          (_, element) => element?.textContent === "namespace=prod",
        ),
      ).toBeInTheDocument(),
    );
  });

  it("uses dedicated visual roles for preview labels and Cancel", async () => {
    const stores = createStores(false);
    global.fetch = jest.fn(async (input: RequestInfo | URL) => {
      if (!input.toString().includes("/silence-editor/preview")) {
        return new Response(JSON.stringify({ silenceID: "unused" }), {
          status: 200,
        });
      }
      return previewResponse([
        {
          ...previewAlert("preview-label-colors"),
          labels: [
            { name: "alertname", value: "preview-label-colors" },
            { name: "service", value: "payments" },
          ],
          matchedLabels: [
            { name: "alertname", value: "preview-label-colors" },
          ],
          matchedLabelNames: ["alertname"],
          missingLabelNames: ["cluster"],
        },
      ]);
    }) as typeof fetch;

    renderEditor(stores);

    await waitFor(() =>
      expect(screen.getByText("service=payments")).toBeInTheDocument(),
    );

    expect(screen.getByText("service=payments")).toHaveClass(
      "fork-silence-editor__preview-label",
    );
    expect(screen.getByText("alertname=preview-label-colors")).toHaveClass(
      "fork-silence-editor__preview-label--matched",
    );
    expect(screen.getByText("cluster=<missing>")).toHaveClass(
      "fork-silence-editor__preview-label--missing",
    );
    expect(screen.getByRole("button", { name: "Cancel" })).toHaveClass(
      "fork-silence-editor__cancel",
    );
  });
});
