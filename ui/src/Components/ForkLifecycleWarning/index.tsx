import type { FC } from "react";

import { useFetchGet } from "Hooks/useFetchGet";
import { FormatBackendURI } from "Stores/AlertStore";
import type { AlertStore } from "Stores/AlertStore";

type WarningResponseT = {
  status: string;
  storageEnabled: boolean;
  dbExpectedState: boolean;
  reason?: string;
  regroupStatus?: string;
  regroupProcessed?: number;
  regroupTotal?: number;
};

const ForkLifecycleWarning: FC<{ alertStore: AlertStore }> = ({ alertStore }) => {
  const { response, error } = useFetchGet<WarningResponseT>(
    FormatBackendURI("alerts-history/warning"),
    {
      deps: [alertStore.info.timestamp],
    },
  );

  if (error || !response) return null;
  if (!response.storageEnabled) return null;
  if (response.dbExpectedState !== false) return null;

  return (
    <div className="alert alert-warning mt-2 mb-2" role="alert">
      <strong>Warning:</strong> lifecycle DB is not in expected grouping state.
      {response.reason ? <span> {response.reason}</span> : null}
      {response.regroupStatus ? (
        <span className="ms-2 text-muted">
          Status: {response.regroupStatus}
          {typeof response.regroupProcessed === "number" &&
          typeof response.regroupTotal === "number"
            ? ` (${response.regroupProcessed}/${response.regroupTotal})`
            : ""}
        </span>
      ) : null}
    </div>
  );
};

export { ForkLifecycleWarning };
