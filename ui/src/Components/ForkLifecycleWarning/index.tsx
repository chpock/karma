import { useEffect, useState, type FC } from "react";
import { CommonOptions } from "Common/Fetch";
import { useFetchGet } from "Hooks/useFetchGet";
import { FormatBackendURI } from "Stores/AlertStore";
import type { AlertStore } from "Stores/AlertStore";
import { observer } from "mobx-react-lite";

type WarningIssueT =
  | {
      kind: "regroup_out_of_date";
      severity: "warning";
      summary: string;
      metadata: { storedFingerprint: string; expectedFingerprint: string };
    }
  | {
      kind: "regroup_in_progress";
      severity: "warning";
      summary: string;
      metadata: { processed: number; total: number };
    }
  | {
      kind: "regroup_failed";
      severity: "error";
      summary: string;
      metadata: { error: string; processed: number; total: number };
    }
  | {
      kind: "silence_ha_inconsistent";
      severity: "warning";
      summary: string;
      metadata: { targetNames: string[] };
    }
  | {
      kind: "recheck_retrying";
      severity: "warning";
      summary: string;
      metadata: {
        jobId: number;
        status: "retry_wait";
        retryCount: number;
        nextAttemptAt: string;
        lastFailureStage: string;
      };
    }
  | {
      kind: "recheck_blocked";
      severity: "error";
      summary: string;
      metadata: {
        jobId: number;
        status: "failed";
        firstFailureStage: string;
        firstFailureAt: string;
      };
    };

type WarningResponseT = {
  status: string;
  storageEnabled: boolean;
  dbExpectedState: boolean;
  issues: WarningIssueT[];
};

const newRequestID = (): string => {
  const cryptoWithUUID = globalThis.crypto as Crypto & {
    randomUUID?: () => string;
  };
  if (cryptoWithUUID.randomUUID) {
    return `ui-${cryptoWithUUID.randomUUID()}`;
  }
  return `ui-${Date.now()}-${Math.random().toString(36).slice(2)}`;
};

const issueMetadata = (issue: WarningIssueT): string => {
  switch (issue.kind) {
    case "regroup_out_of_date":
      return `Stored ${issue.metadata.storedFingerprint}; expected ${issue.metadata.expectedFingerprint}`;
    case "regroup_in_progress":
      return `Progress ${issue.metadata.processed}/${issue.metadata.total}`;
    case "regroup_failed":
      return `${issue.metadata.error} (${issue.metadata.processed}/${issue.metadata.total})`;
    case "silence_ha_inconsistent":
      return `Targets: ${issue.metadata.targetNames.join(", ")}`;
    case "recheck_retrying":
      return `Job ${issue.metadata.jobId}, retry ${issue.metadata.retryCount}; next attempt ${issue.metadata.nextAttemptAt}; stage ${issue.metadata.lastFailureStage}`;
    case "recheck_blocked":
      return `Job ${issue.metadata.jobId}; first failure at ${issue.metadata.firstFailureAt} during ${issue.metadata.firstFailureStage}`;
  }
};

const operatorErrorMessage = async (response: Response): Promise<string> => {
  const fallback = `${response.status} ${response.statusText || "request failed"}`;
  const contentType = response.headers.get("content-type") || "";
  if (!contentType.toLowerCase().includes("application/json")) return fallback;
  try {
    const body: unknown = await response.json();
    if (
      typeof body === "object" &&
      body !== null &&
      "error" in body &&
      typeof body.error === "string" &&
      body.error.trim() !== ""
    ) {
      return body.error;
    }
  } catch {
    // Use the stable HTTP fallback for malformed operator error envelopes.
  }
  return fallback;
};

const ForkLifecycleWarning: FC<{ alertStore: AlertStore }> = observer(
  ({ alertStore }) => {
    const { response, error, status, get } = useFetchGet<WarningResponseT>(
      FormatBackendURI("alerts-history/warning"),
      {
        deps: [alertStore.info.timestamp],
      },
    );
    const [confirmedResponse, setConfirmedResponse] =
      useState<WarningResponseT | null>(null);
    const [action, setAction] = useState<"retry" | "abandon" | null>(null);
    const [confirmAbandon, setConfirmAbandon] = useState(false);
    const [abandonReason, setAbandonReason] = useState("");
    const [actionError, setActionError] = useState<string | null>(null);

    useEffect(() => {
      if (response !== null) {
        setConfirmedResponse(response);
      }
    }, [response]);

    useEffect(() => {
      if (status === 401) {
        alertStore.info.invalidateAuthentication();
      }
    }, [alertStore, status]);

    if (!confirmedResponse) return null;
    if (!confirmedResponse.storageEnabled) return null;
    if (confirmedResponse.issues.length === 0) return null;

    const blockedIssue = confirmedResponse.issues.find(
      (issue): issue is Extract<WarningIssueT, { kind: "recheck_blocked" }> =>
        issue.kind === "recheck_blocked",
    );
    const canAct =
      alertStore.info.authenticationReady &&
      (!alertStore.info.authentication.enabled ||
        alertStore.info.authentication.username.trim() !== "");
    const submitAction = async (requestedAction: "retry" | "abandon") => {
      if (action || !blockedIssue || !canAct) return;
      if (requestedAction === "abandon" && abandonReason.trim() === "") return;
      setAction(requestedAction);
      setActionError(null);
      try {
        const body: { requestId: string; reason?: string } = {
          requestId: newRequestID(),
        };
        if (requestedAction === "abandon") body.reason = abandonReason.trim();
        const result = await fetch(
          FormatBackendURI(
            `alerts-history/recheck/${blockedIssue.metadata.jobId}/${requestedAction}`,
          ),
          {
            ...CommonOptions,
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              "X-Karma-Request": "recheck-operator",
            },
            body: JSON.stringify(body),
          },
        );
        if (result.status === 401) {
          alertStore.info.invalidateAuthentication();
          setActionError("Authentication required");
          await get();
        } else if (result.status === 409) {
          setActionError(
            "The queue changed on the server. The warning was refreshed.",
          );
          await get();
        } else if (!result.ok) {
          const message = await operatorErrorMessage(result);
          setActionError(message || `${result.status} ${result.statusText}`);
          await get();
        } else {
          setConfirmAbandon(false);
          setAbandonReason("");
          setActionError(null);
          await get();
        }
      } catch (requestError) {
        setActionError(
          requestError instanceof Error
            ? requestError.message
            : "Operator action failed",
        );
        await get();
      } finally {
        setAction(null);
      }
    };

    return (
      <div
        className="fork-lifecycle-warning mt-2 mb-2"
        role="status"
        aria-live="polite"
      >
        {confirmedResponse.issues.map((issue) => (
          <div
            className={`alert ${issue.severity === "error" ? "alert-danger" : "alert-warning"} py-2 mb-2`}
            data-issue-kind={issue.kind}
            key={issue.kind}
            role="alert"
          >
            <strong>{issue.summary}</strong>
            <div className="small">{issueMetadata(issue)}</div>
            {issue.kind === "recheck_blocked" && canAct ? (
              <div className="mt-2">
                <button
                  type="button"
                  className="btn btn-primary btn-sm me-2"
                  disabled={action !== null}
                  onClick={() => void submitAction("retry")}
                >
                  {action === "retry" ? "Retrying..." : "Retry"}
                </button>
                {!confirmAbandon ? (
                  <button
                    type="button"
                    className="fork-lifecycle-warning-abandon btn btn-danger btn-sm"
                    disabled={action !== null}
                    onClick={() => setConfirmAbandon(true)}
                  >
                    Abandon
                  </button>
                ) : (
                  <div className="mt-2">
                    <label
                      className="form-label"
                      htmlFor="recheck-abandon-reason"
                    >
                      Abandon reason
                    </label>
                    <input
                      id="recheck-abandon-reason"
                      className="form-control form-control-sm mb-2"
                      value={abandonReason}
                      onChange={(event) => setAbandonReason(event.target.value)}
                      disabled={action !== null}
                    />
                    <button
                      type="button"
                      className="fork-lifecycle-warning-abandon btn btn-danger btn-sm me-2"
                      disabled={action !== null || abandonReason.trim() === ""}
                      onClick={() => void submitAction("abandon")}
                    >
                      {action === "abandon"
                        ? "Abandoning..."
                        : "Confirm abandon"}
                    </button>
                    <button
                      type="button"
                      className="btn btn-secondary btn-sm"
                      disabled={action !== null}
                      onClick={() => {
                        setConfirmAbandon(false);
                        setAbandonReason("");
                      }}
                    >
                      Cancel
                    </button>
                  </div>
                )}
              </div>
            ) : null}
          </div>
        ))}
        {error ? (
          <div className="fork-lifecycle-warning-error small mt-1">
            Warning refresh failed: {error}
          </div>
        ) : null}
        {actionError ? (
          <div className="fork-lifecycle-warning-error small mt-1">
            {actionError}
          </div>
        ) : null}
      </div>
    );
  },
);

export { ForkLifecycleWarning };
