import { act } from "react";

import {
  render,
  fireEvent,
  screen,
  waitFor,
  within,
} from "@testing-library/react";
import fetchMock from "@fetch-mock/jest";

import { AlertStore } from "Stores/AlertStore";
import { EmptyAPIResponse } from "__fixtures__/Fetch";
import { useFetchGetMock } from "__fixtures__/useFetchGet";
import { useFetchGet } from "Hooks/useFetchGet";
import { ForkLifecycleWarning } from ".";

const blockedWarning = {
  status: "ok",
  storageEnabled: true,
  dbExpectedState: false,
  issues: [
    {
      kind: "regroup_in_progress",
      severity: "warning",
      summary: "Regroup in progress",
      metadata: { processed: 2, total: 4 },
    },
    {
      kind: "recheck_blocked",
      severity: "error",
      summary: "Recheck queue is blocked",
      metadata: {
        jobId: 547,
        status: "failed",
        firstFailureStage: "persist_candidate",
        firstFailureAt: "2026-08-19T12:00:00Z",
      },
    },
  ],
};

const recoveredWarning = {
  ...blockedWarning,
  issues: [
    {
      kind: "recheck_retrying" as const,
      severity: "warning" as const,
      summary: "Recheck queue is retrying",
      metadata: {
        jobId: 547,
        status: "retry_wait" as const,
        retryCount: 1,
        nextAttemptAt: "2026-08-19T12:01:00Z",
        lastFailureStage: "persist_candidate",
      },
    },
  ],
};

const clearedWarning = { ...blockedWarning, issues: [] };

const allWarningIssues = {
  ...blockedWarning,
  issues: [
    {
      kind: "regroup_out_of_date",
      severity: "warning",
      summary: "Regroup is out of date",
      metadata: { storedFingerprint: "old", expectedFingerprint: "new" },
    },
    blockedWarning.issues[0],
    {
      kind: "regroup_failed",
      severity: "error",
      summary: "Regroup failed",
      metadata: { error: "writer unavailable", processed: 3, total: 4 },
    },
    {
      kind: "silence_ha_inconsistent",
      severity: "warning",
      summary: "Silence state is inconsistent",
      metadata: { targetNames: ["production", "staging"] },
    },
    recoveredWarning.issues[0],
    blockedWarning.issues[1],
  ],
};

let alertStore: AlertStore;

beforeEach(() => {
  alertStore = new AlertStore([]);
  const alertsResponse = EmptyAPIResponse();
  alertsResponse.filters = [];
  alertsResponse.authentication = {
    enabled: true,
    username: "operator@example.com",
  };
  alertStore.parseAPIResponse(alertsResponse);
  fetchMock.mockReset();
});

afterEach(() => {
  fetchMock.mockReset();
  jest.clearAllMocks();
});

const renderWarning = () =>
  render(<ForkLifecycleWarning alertStore={alertStore} />);

const setWarningResponse = (response: unknown) => {
  setWarningState(response, null);
};

const setWarningState = (response: unknown, error: string | null) => {
  useFetchGetMock.fetch.setMockedData({
    ...useFetchGetMock.fetch.mockedData,
    response: response as never,
    error,
    isLoading: false,
  });
};

const flushFetches = async () => {
  await act(async () => {
    await fetchMock.callHistory.flush(true);
  });
};

describe("<ForkLifecycleWarning />", () => {
  it("renders all six warning issue kinds", async () => {
    setWarningResponse(allWarningIssues);

    renderWarning();
    await act(async () => {});

    expect(screen.getByText("Regroup is out of date")).toBeInTheDocument();
    expect(screen.getByText("Regroup in progress")).toBeInTheDocument();
    expect(screen.getByText("Regroup failed")).toBeInTheDocument();
    expect(
      screen.getByText("Silence state is inconsistent"),
    ).toBeInTheDocument();
    expect(screen.getByText("Recheck queue is retrying")).toBeInTheDocument();
    expect(screen.getByText("Recheck queue is blocked")).toBeInTheDocument();
  });

  it("renders exact metadata and semantic severity for every issue kind", async () => {
    setWarningResponse(allWarningIssues);

    renderWarning();
    await act(async () => {});

    const expected = [
      ["Regroup is out of date", "Stored old; expected new", "alert-warning"],
      ["Regroup in progress", "Progress 2/4", "alert-warning"],
      ["Regroup failed", "writer unavailable (3/4)", "alert-danger"],
      [
        "Silence state is inconsistent",
        "Targets: production, staging",
        "alert-warning",
      ],
      [
        "Recheck queue is retrying",
        "Job 547, retry 1; next attempt 2026-08-19T12:01:00Z; stage persist_candidate",
        "alert-warning",
      ],
      [
        "Recheck queue is blocked",
        "Job 547; first failure at 2026-08-19T12:00:00Z during persist_candidate",
        "alert-danger",
      ],
    ] as const;
    for (const [summary, metadata, severityClass] of expected) {
      const issue = screen.getByText(summary).closest('[role="alert"]');
      expect(issue).not.toBeNull();
      expect(issue).toHaveClass(severityClass);
      expect(
        within(issue as HTMLElement).getByText(metadata),
      ).toBeInTheDocument();
    }
  });

  it("keeps controls hidden for a retrying head", async () => {
    setWarningResponse(recoveredWarning);

    renderWarning();
    await act(async () => {});

    expect(screen.getByText("Recheck queue is retrying")).toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: "Retry" }),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: "Abandon" }),
    ).not.toBeInTheDocument();
  });

  it("renders every simultaneous issue and exposes controls only for the failed head", async () => {
    setWarningResponse(blockedWarning);

    renderWarning();
    await act(async () => {});

    expect(screen.getAllByRole("alert")).toHaveLength(2);
    expect(screen.getByText("Regroup in progress")).toBeInTheDocument();
    expect(screen.getByText("Recheck queue is blocked")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Retry" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Abandon" })).toBeInTheDocument();
  });

  it("confirms abandonment, sends the reason, and refreshes after success", async () => {
    setWarningResponse(blockedWarning);
    fetchMock.route(
      /alerts-history\/recheck/,
      { status: 200, body: JSON.stringify({ status: "success" }) },
      { method: "POST" },
    );

    renderWarning();
    await act(async () => {});
    fireEvent.click(screen.getByRole("button", { name: "Abandon" }));
    expect(
      screen.getByRole("button", { name: "Confirm abandon" }),
    ).toBeDisabled();

    fireEvent.change(screen.getByLabelText("Abandon reason"), {
      target: { value: "Release the blocked head" },
    });
    expect(
      screen.getByRole("button", { name: "Confirm abandon" }),
    ).toBeEnabled();
    fireEvent.click(screen.getByRole("button", { name: "Confirm abandon" }));
    setWarningResponse(clearedWarning);
    await flushFetches();

    const actionCall = fetchMock.callHistory.lastCall();
    expect(actionCall?.url).toContain("/alerts-history/recheck/547/abandon");
    expect(JSON.parse(String(actionCall?.options?.body))).toMatchObject({
      reason: "Release the blocked head",
    });
    expect(useFetchGetMock.fetch.calls.length).toBeGreaterThanOrEqual(2);
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
  });

  it("refreshes after a conflict and prevents a second request while retry is pending", async () => {
    setWarningResponse(blockedWarning);
    fetchMock.route(
      /alerts-history\/recheck/,
      { status: 409, body: "queue changed" },
      { method: "POST" },
    );

    renderWarning();
    await act(async () => {});
    const retry = screen.getByRole("button", { name: "Retry" });
    fireEvent.click(retry);
    fireEvent.click(retry);
    expect(screen.getByRole("button", { name: "Retrying..." })).toBeDisabled();
    await flushFetches();

    expect(
      screen.getByText(
        "The queue changed on the server. The warning was refreshed.",
      ),
    ).toBeInTheDocument();
    expect(fetchMock.callHistory.calls()).toHaveLength(1);
    expect(useFetchGetMock.fetch.calls.length).toBeGreaterThanOrEqual(2);
    expect(screen.getByRole("button", { name: "Retry" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Abandon" })).toBeInTheDocument();
  });

  it("replaces the warning with the authoritative response after a successful retry", async () => {
    setWarningResponse(blockedWarning);
    fetchMock.route(
      /alerts-history\/recheck/,
      { status: 200, body: JSON.stringify({ status: "success" }) },
      { method: "POST" },
    );

    renderWarning();
    await act(async () => {});
    fireEvent.click(screen.getByRole("button", { name: "Retry" }));
    setWarningResponse(clearedWarning);
    await flushFetches();

    expect(
      screen.queryByText("Recheck queue is blocked"),
    ).not.toBeInTheDocument();
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
  });

  it("cancels abandon confirmation without sending an action", async () => {
    setWarningResponse(blockedWarning);

    renderWarning();
    await act(async () => {});
    fireEvent.click(screen.getByRole("button", { name: "Abandon" }));
    expect(screen.getByLabelText("Abandon reason")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Cancel" }));

    expect(screen.queryByLabelText("Abandon reason")).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Abandon" })).toBeInTheDocument();
    expect(fetchMock.callHistory.calls()).toHaveLength(0);
  });

  it("renders a refreshed authoritative warning after a successful conflict refresh", async () => {
    setWarningResponse(blockedWarning);
    fetchMock.route(
      /alerts-history\/recheck/,
      { status: 409, body: "queue changed" },
      { method: "POST" },
    );

    renderWarning();
    await act(async () => {});
    fireEvent.click(screen.getByRole("button", { name: "Retry" }));
    setWarningResponse(recoveredWarning);
    await flushFetches();

    expect(
      screen.queryByText("Recheck queue is blocked"),
    ).not.toBeInTheDocument();
    expect(screen.getByText("Recheck queue is retrying")).toBeInTheDocument();
    expect(
      screen.queryByText("Warning refresh failed: warning refresh failed"),
    ).not.toBeInTheDocument();
  });

  it("keeps the confirmed warning and shows action and refresh errors after rejection", async () => {
    setWarningResponse(blockedWarning);
    fetchMock.route(
      /alerts-history\/recheck/,
      {
        status: 400,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ error: "operator rejected" }),
      },
      { method: "POST" },
    );

    renderWarning();
    await act(async () => {});
    fireEvent.click(screen.getByRole("button", { name: "Retry" }));
    setWarningState(undefined, "warning refresh failed");
    await flushFetches();

    expect(screen.getByText("Recheck queue is blocked")).toBeInTheDocument();
    expect(screen.getByText("operator rejected")).toHaveClass(
      "fork-lifecycle-warning-error",
    );
    expect(
      screen.getByText("Warning refresh failed: warning refresh failed"),
    ).toHaveClass("fork-lifecycle-warning-error");
    expect(
      screen.getByText("Warning refresh failed: warning refresh failed"),
    ).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Retry" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Abandon" })).toBeInTheDocument();
  });

  it("uses a stable status fallback for malformed operator JSON errors", async () => {
    setWarningResponse(blockedWarning);
    fetchMock.route(
      /alerts-history\/recheck/,
      {
        status: 500,
        headers: { "Content-Type": "application/json" },
        body: "{malformed",
      },
      { method: "POST" },
    );

    renderWarning();
    await act(async () => {});
    fireEvent.click(screen.getByRole("button", { name: "Retry" }));
    setWarningState(undefined, "warning refresh failed");
    await flushFetches();

    expect(screen.getByText("500 Internal Server Error")).toBeInTheDocument();
    expect(screen.queryByText("{malformed")).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Retry" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Abandon" })).toBeInTheDocument();
  });

  it("keeps the confirmed warning and shows action and refresh errors after a network failure", async () => {
    setWarningResponse(blockedWarning);
    fetchMock.route(
      /alerts-history\/recheck/,
      { throws: new Error("network down") },
      { method: "POST" },
    );

    renderWarning();
    await act(async () => {});
    fireEvent.click(screen.getByRole("button", { name: "Retry" }));
    setWarningState(undefined, "warning refresh failed");
    await flushFetches();

    expect(screen.getByText("Recheck queue is blocked")).toBeInTheDocument();
    expect(screen.getByText("network down")).toBeInTheDocument();
    expect(
      screen.getByText("Warning refresh failed: warning refresh failed"),
    ).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Retry" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Abandon" })).toBeInTheDocument();
  });

  it("preserves the confirmed warning after a failed conflict refresh and recovers later", async () => {
    setWarningResponse(blockedWarning);
    fetchMock.route(
      /alerts-history\/recheck/,
      { status: 409, body: "queue changed" },
      { method: "POST" },
    );

    renderWarning();
    await act(async () => {});
    fireEvent.click(screen.getByRole("button", { name: "Retry" }));
    setWarningState(undefined, "warning refresh failed");
    await flushFetches();

    expect(screen.getByText("Recheck queue is blocked")).toBeInTheDocument();
    expect(
      screen.getByText("Warning refresh failed: warning refresh failed"),
    ).toBeInTheDocument();

    setWarningResponse(recoveredWarning);
    await act(async () => {
      alertStore.info.setTimestamp("2026-08-19T12:02:00Z");
    });

    expect(
      screen.queryByText("Recheck queue is blocked"),
    ).not.toBeInTheDocument();
    expect(screen.getByText("Recheck queue is retrying")).toBeInTheDocument();
  });

  it("keeps the newest refresh authoritative over older success and failure resolutions", async () => {
    const actualUseFetchGet = jest.requireActual("Hooks/useFetchGet")
      .useFetchGet as typeof useFetchGet;
    (
      useFetchGet as jest.MockedFunction<typeof useFetchGetMock>
    ).mockImplementation(actualUseFetchGet);
    const pending: Array<(response: Response) => void> = [];
    fetchMock.route(
      /alerts-history\/warning/,
      () => new Promise<Response>((resolve) => pending.push(resolve)),
      { method: "GET" },
    );
    renderWarning();
    await act(async () => {});
    expect(pending).toHaveLength(1);

    await act(async () => {
      pending[0](
        new Response(JSON.stringify(blockedWarning), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        }),
      );
    });
    expect(screen.getByText("Recheck queue is blocked")).toBeInTheDocument();

    await act(async () => {
      alertStore.info.setTimestamp("2026-08-19T12:01:00Z");
    });
    await act(async () => {
      alertStore.info.setTimestamp("2026-08-19T12:02:00Z");
    });
    expect(pending).toHaveLength(3);

    await act(async () => {
      pending[2](
        new Response(JSON.stringify(recoveredWarning), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        }),
      );
    });
    expect(screen.getByText("Recheck queue is retrying")).toBeInTheDocument();

    await act(async () => {
      pending[1](
        new Response("{malformed", {
          status: 500,
          headers: { "Content-Type": "application/json" },
        }),
      );
    });
    expect(screen.getByText("Recheck queue is retrying")).toBeInTheDocument();
    expect(
      screen.queryByText(/Warning refresh failed: 500/),
    ).not.toBeInTheDocument();

    await act(async () => {
      alertStore.info.setTimestamp("2026-08-19T12:03:00Z");
    });
    await act(async () => {
      alertStore.info.setTimestamp("2026-08-19T12:04:00Z");
    });
    expect(pending).toHaveLength(5);
    await act(async () => {
      pending[4](
        new Response(JSON.stringify(recoveredWarning), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        }),
      );
    });
    await act(async () => {
      pending[3](
        new Response(JSON.stringify(blockedWarning), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        }),
      );
    });
    expect(screen.getByText("Recheck queue is retrying")).toBeInTheDocument();
    expect(
      screen.queryByText("Recheck queue is blocked"),
    ).not.toBeInTheDocument();
  });

  it.each([
    ["success", () => new Response(JSON.stringify(blockedWarning), { status: 200, headers: { "Content-Type": "application/json" } })],
    ["HTTP rejection", () => new Response(JSON.stringify({ error: "old internal detail" }), { status: 500, headers: { "Content-Type": "application/json" } })],
    ["parse failure", () => new Response("{malformed", { status: 200, headers: { "Content-Type": "application/json" } })],
  ])("keeps post-action refresh authoritative over an older automatic %s", async (_name, oldResponse) => {
    const actualUseFetchGet = jest.requireActual("Hooks/useFetchGet")
      .useFetchGet as typeof useFetchGet;
    (useFetchGet as jest.MockedFunction<typeof useFetchGetMock>).mockImplementation(actualUseFetchGet);
    const pending: Array<(response: Response) => void> = [];
    fetchMock.route(/alerts-history\/warning/, () => new Promise<Response>((resolve) => pending.push(resolve)), { method: "GET" });
    fetchMock.route(/alerts-history\/recheck\/547\/retry/, { status: 200, body: "{}" }, { method: "POST" });
    renderWarning();
    await waitFor(() => expect(pending).toHaveLength(1));
    await act(async () => {
      pending[0](new Response(JSON.stringify(blockedWarning), { status: 200, headers: { "Content-Type": "application/json" } }));
    });
    await waitFor(() => expect(screen.getByRole("button", { name: "Retry" })).toBeInTheDocument());
    await act(async () => {
      alertStore.info.setTimestamp("2026-08-19T13:00:00Z");
    });
    await waitFor(() => expect(pending).toHaveLength(2));
    fireEvent.click(screen.getByRole("button", { name: "Retry" }));
    await waitFor(() => expect(pending).toHaveLength(3));
    await act(async () => {
      pending[2](new Response(JSON.stringify(recoveredWarning), { status: 200, headers: { "Content-Type": "application/json" } }));
    });
    expect(screen.getByText("Recheck queue is retrying")).toBeInTheDocument();
    await act(async () => {
      pending[1](oldResponse());
    });
    expect(screen.getByText("Recheck queue is retrying")).toBeInTheDocument();
    expect(screen.queryByText("Recheck queue is blocked")).not.toBeInTheDocument();
    expect(screen.queryByText(/old internal detail/)).not.toBeInTheDocument();
  });

  it("preserves confirmed warning data across a failed post-action refresh", async () => {
    const actualUseFetchGet = jest.requireActual("Hooks/useFetchGet")
      .useFetchGet as typeof useFetchGet;
    (useFetchGet as jest.MockedFunction<typeof useFetchGetMock>).mockImplementation(actualUseFetchGet);
    const pending: Array<(response: Response) => void> = [];
    fetchMock.route(/alerts-history\/warning/, () => new Promise<Response>((resolve) => pending.push(resolve)), { method: "GET" });
    fetchMock.route(/alerts-history\/recheck\/547\/retry/, { status: 200, body: "{}" }, { method: "POST" });
    renderWarning();
    await waitFor(() => expect(pending).toHaveLength(1));
    await act(async () => {
      pending[0](new Response(JSON.stringify(blockedWarning), { status: 200, headers: { "Content-Type": "application/json" } }));
    });
    await waitFor(() => expect(screen.getByRole("button", { name: "Retry" })).toBeInTheDocument());
    fireEvent.click(screen.getByRole("button", { name: "Retry" }));
    await waitFor(() => expect(pending).toHaveLength(2));
    await act(async () => {
      pending[1](new Response(JSON.stringify({ error: "database internals" }), { status: 500, headers: { "Content-Type": "application/json" } }));
    });
    expect(screen.getByText("Recheck queue is blocked")).toBeInTheDocument();
    expect(screen.getByText(/Warning refresh failed: 500/)).toBeInTheDocument();
    expect(screen.queryByText(/database internals/)).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Retry" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Abandon" })).toBeInTheDocument();
    await act(async () => {
      alertStore.info.setTimestamp("2026-08-19T13:01:00Z");
    });
    await waitFor(() => expect(pending).toHaveLength(3));
    await act(async () => {
      pending[2](new Response(JSON.stringify(recoveredWarning), { status: 200, headers: { "Content-Type": "application/json" } }));
    });
    expect(screen.getByText("Recheck queue is retrying")).toBeInTheDocument();
    expect(screen.queryByText(/Warning refresh failed/)).not.toBeInTheDocument();
  });

  it("keeps controls hidden when warning data arrives before authentication readiness", async () => {
    alertStore = new AlertStore([]);
    setWarningResponse(blockedWarning);

    renderWarning();
    await act(async () => {});

    expect(screen.getByText("Recheck queue is blocked")).toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: "Retry" }),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: "Abandon" }),
    ).not.toBeInTheDocument();

    const alertsResponse = EmptyAPIResponse();
    alertsResponse.filters = [];
    alertsResponse.authentication = {
      enabled: true,
      username: "operator@example.com",
    };
    await act(async () => {
      alertStore.parseAPIResponse(alertsResponse);
    });

    expect(screen.getByRole("button", { name: "Retry" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Abandon" })).toBeInTheDocument();
  });

  it("does not expose operator controls without an authenticated session", async () => {
    alertStore.info.setAuthentication(true, "");
    setWarningResponse(blockedWarning);

    renderWarning();
    await act(async () => {});

    expect(
      screen.queryByRole("button", { name: "Retry" }),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: "Abandon" }),
    ).not.toBeInTheDocument();
  });

  it("hides controls immediately after operator 401 and re-enables them after auth recovery", async () => {
    setWarningResponse(blockedWarning);
    fetchMock.route(
      /alerts-history\/recheck/,
      {
        status: 401,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ error: "authentication required" }),
      },
      { method: "POST" },
    );

    renderWarning();
    await act(async () => {});
    fireEvent.click(screen.getByRole("button", { name: "Retry" }));
    await flushFetches();

    expect(screen.getByText("Recheck queue is blocked")).toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: "Retry" }),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: "Abandon" }),
    ).not.toBeInTheDocument();

    const authRecovery = EmptyAPIResponse();
    authRecovery.filters = [];
    authRecovery.authentication = {
      enabled: true,
      username: "operator@example.com",
    };
    await act(async () => {
      alertStore.parseAPIResponse(authRecovery);
    });

    expect(screen.getByRole("button", { name: "Retry" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Abandon" })).toBeInTheDocument();
  });

  it("exposes operator controls when authoritative authentication is disabled", async () => {
    alertStore.info.setAuthentication(false, "");
    setWarningResponse(blockedWarning);

    renderWarning();
    await act(async () => {});

    expect(screen.getByRole("button", { name: "Retry" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Abandon" })).toBeInTheDocument();
  });
});
