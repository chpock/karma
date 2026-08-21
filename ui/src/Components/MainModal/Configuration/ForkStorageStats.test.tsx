import { act } from "react";

import { render, screen, waitFor } from "@testing-library/react";
import fetchMock from "@fetch-mock/jest";

import { useFetchGetMock } from "__fixtures__/useFetchGet";
import { AlertStore } from "Stores/AlertStore";
import { useFetchGet } from "Hooks/useFetchGet";
import { ForkStorageStats } from "./ForkStorageStats";

beforeEach(() => {
	fetchMock.mockReset();
  useFetchGetMock.fetch.setMockedData({
    ...useFetchGetMock.fetch.mockedData,
    response: {
      status: "ok",
      storageEnabled: true,
      sqlitePath: "/tmp/history.sqlite",
      retentionTTLHours: 720,
      maxAlerts: 1000,
      stateCounts: {},
      recheck: {
        queueDepth: 1,
        headJob: {
          jobId: 547,
          triggerType: "source_context_changed",
          status: "retry_wait",
          createdAt: "2026-08-19T12:00:00Z",
          startedAt: "2026-08-19T12:00:01Z",
          finishedAt: null,
          totalCandidates: 20,
          processedCandidates: 5,
          outcomeCounts: {
            promoted: 3,
            stale: 1,
            ineligible: 1,
            missingLabels: 0,
            incompleteSources: 0,
          },
          attemptCount: 2,
          retryCount: 1,
          consecutiveRetryCount: 1,
          nextAttemptAt: "2026-08-19T12:00:05Z",
          firstError: null,
          lastError: {
            class: "retryable_storage",
            stage: "persist_candidate",
            at: "2026-08-19T12:00:02Z",
            message: "busy",
          },
          recoveryCount: 0,
        },
      },
    } as never,
    error: null,
    isLoading: false,
  });
});

afterEach(() => {
	fetchMock.mockReset();
	jest.clearAllMocks();
});

it("renders the nested recheck head without deriving state from legacy fields", async () => {
  const alertStore = new AlertStore([]);
  render(<ForkStorageStats alertStore={alertStore} />);
  await act(async () => {});

  expect(screen.getByText(/Recheck queue:/)).toBeInTheDocument();
  expect(screen.getByText(/head job 547, retry_wait/)).toBeInTheDocument();
  expect(screen.getByText("5/20")).toBeInTheDocument();
  expect(screen.getByText("retryable_storage/persist_candidate")).toBeInTheDocument();
  expect(screen.queryByText(/recheckQueueDepth/)).not.toBeInTheDocument();
});

it("renders a safe textual HTTP fallback for a real JSON 500 envelope", async () => {
	const actualUseFetchGet = jest.requireActual("Hooks/useFetchGet")
		.useFetchGet as typeof useFetchGet;
	(useFetchGet as jest.MockedFunction<typeof useFetchGetMock>).mockImplementation(actualUseFetchGet);
	fetchMock.route(/alerts-history\/stats/, {
		status: 500,
		headers: { "Content-Type": "application/json" },
		body: JSON.stringify({ error: "SQL query and token must stay private" }),
	});
	render(<ForkStorageStats alertStore={new AlertStore([])} />);
	await waitFor(() => expect(screen.getByText("500 Internal Server Error")).toBeInTheDocument());

	expect(screen.getByText("500 Internal Server Error")).toBeInTheDocument();
	expect(screen.queryByText(/SQL query|token must stay private/)).not.toBeInTheDocument();
  expect(screen.queryByText(/ErrorBoundary|Unexpected error/)).not.toBeInTheDocument();
});
