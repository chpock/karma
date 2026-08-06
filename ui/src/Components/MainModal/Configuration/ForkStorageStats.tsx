import type { FC } from "react";

import { observer } from "mobx-react-lite";

import type { AlertStore } from "Stores/AlertStore";
import type { APIAlertLifecycleStatsResponseT } from "Models/APITypes";
import { FormatBackendURI } from "Stores/AlertStore";
import { useFetchGet } from "Hooks/useFetchGet";

const formatBytes = (bytes: number): string => {
  if (!Number.isFinite(bytes) || bytes <= 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let value = bytes;
  let idx = 0;
  while (value >= 1024 && idx < units.length - 1) {
    value /= 1024;
    idx++;
  }
  return `${value.toFixed(idx === 0 ? 0 : 2)} ${units[idx]}`;
};

const valueOrDash = (v: number | undefined): string => {
  if (typeof v !== "number") return "-";
  return `${v}`;
};

const stateValueOrZero = (v: number | undefined): string =>
  typeof v !== "number" ? "0" : `${v}`;

const titleCase = (v: string): string =>
  v.length > 0 ? v[0].toUpperCase() + v.slice(1) : v;

const isUnknownState = (state: string): boolean =>
  state.trim().toLowerCase() === "unknown";

const timeOrDash = (ts: string | null | undefined): string => {
  if (!ts) return "-";
  const date = new Date(ts);
  if (Number.isNaN(date.getTime())) return ts;
  return date.toLocaleString();
};

const ForkStorageStats: FC<{
  alertStore: AlertStore;
}> = observer(({ alertStore }) => {
  const { response, error, isLoading } = useFetchGet<APIAlertLifecycleStatsResponseT>(
    FormatBackendURI("alerts-history/stats"),
    {
      deps: [alertStore.info.timestamp],
    },
  );

  if (error) {
    return <div className="text-danger">{error}</div>;
  }

  if (isLoading || response === null) {
    return <div className="text-muted">Loading storage stats...</div>;
  }

  const stateEntries = Object.entries(response.stateCounts || {}).sort(
    ([a], [b]) => a.localeCompare(b),
  );

  return (
    <div className="small">
      {response.dbExpectedState === false ? (
        <div className="alert alert-warning py-2 mb-3">
          <strong>Warning:</strong> lifecycle DB is not in expected grouping state.
          {response.dbExpectedStateReason ? (
            <span> {response.dbExpectedStateReason}</span>
          ) : null}
        </div>
      ) : null}
      <div className="mb-2">
        <span className="text-muted">Storage enabled:</span>{" "}
        <strong>{response.storageEnabled ? "yes" : "no"}</strong>
      </div>
      <div className="mb-2">
        <span className="text-muted">DB expected state:</span>{" "}
        <strong>{response.dbExpectedState === false ? "no" : "yes"}</strong>
      </div>
      <div className="mb-2">
        <span className="text-muted">Regroup status:</span>{" "}
        <strong>{response.regroupStatus || "-"}</strong>
        {typeof response.regroupProcessed === "number" &&
        typeof response.regroupTotal === "number" ? (
          <span className="text-muted">
            {" "}
            ({response.regroupProcessed}/{response.regroupTotal})
          </span>
        ) : null}
      </div>
      <div className="mb-2">
        <span className="text-muted">Regroup started:</span>{" "}
        <strong>{timeOrDash(response.lastRegroupStartedAt)}</strong>
      </div>
      <div className="mb-2">
        <span className="text-muted">Regroup finished:</span>{" "}
        <strong>{timeOrDash(response.lastRegroupFinishedAt)}</strong>
      </div>
      <div className="mb-2">
        <span className="text-muted">SQLite path:</span>{" "}
        <code>{response.sqlitePath}</code>
      </div>
      <div className="mb-2">
        <span className="text-muted">Retention:</span>{" "}
        <strong>{response.retentionTTLHours}h</strong>
        <span className="text-muted">, max alerts:</span>{" "}
        <strong>{response.maxAlerts}</strong>
      </div>
      <div className="mb-2">
        <span className="text-muted">Current alerts:</span>{" "}
        <strong>{valueOrDash(response.currentRows)}</strong>
        <span className="text-muted">, history rows:</span>{" "}
        <strong>{valueOrDash(response.historyRows)}</strong>
      </div>
      <div className="mb-2">
        <span className="text-muted">By state:</span>{" "}
        {stateEntries.length === 0 ? (
          <strong>none</strong>
        ) : (
          stateEntries.map(([state, count], idx) => (
            <span key={state}>
              {idx > 0 ? <span className="text-muted">, </span> : null}
              {isUnknownState(state) ? (
                <span className="badge bg-warning text-dark">
                  {titleCase(state)} {stateValueOrZero(count)}
                </span>
              ) : (
                <strong>
                  {titleCase(state)} {stateValueOrZero(count)}
                </strong>
              )}
            </span>
          ))
        )}
      </div>
      <div className="mb-2">
        <span className="text-muted">Suppressed split:</span>{" "}
        <strong>
          current {stateValueOrZero(response.suppressedBreakdown?.current)}
        </strong>
        <span className="text-muted">, </span>
        <strong>past {stateValueOrZero(response.suppressedBreakdown?.past)}</strong>
      </div>
      <div className="mb-2">
        <span className="text-muted">DB size:</span>{" "}
        <strong>
          {typeof response.fileSizeBytes === "number"
            ? formatBytes(response.fileSizeBytes)
            : "-"}
        </strong>
        <span className="text-muted"> (</span>
        <code>{valueOrDash(response.fileSizeBytes)}</code>
        <span className="text-muted"> bytes)</span>
      </div>
      <div className="mb-2">
        <span className="text-muted">SQLite pages:</span>{" "}
        <strong>{valueOrDash(response.pageCount)}</strong>
        <span className="text-muted">, page size:</span>{" "}
        <strong>{valueOrDash(response.pageSizeBytes)}</strong>
        <span className="text-muted"> bytes, freelist:</span>{" "}
        <strong>{valueOrDash(response.freelistCount)}</strong>
      </div>
      <div className="mb-1">
        <span className="text-muted">Last housekeeping:</span>{" "}
        <strong>{timeOrDash(response.lastHousekeepingAt)}</strong>
      </div>
      <div>
        <span className="text-muted">Last incremental vacuum:</span>{" "}
        <strong>{timeOrDash(response.lastIncrementalVacuumAt)}</strong>
      </div>
    </div>
  );
});

export { ForkStorageStats };
