import { FC, useEffect, useRef, useState } from "react";

import { parseISO } from "date-fns/parseISO";
import { getUnixTime } from "date-fns/getUnixTime";
import { runInAction } from "mobx";

import type {
  APISilenceT,
  APIAlertmanagerUpstreamT,
  SilenceEditorStatusResponseT,
} from "Models/APITypes";
import { CommonOptions } from "Common/Fetch";
import type { AlertStore } from "Stores/AlertStore";
import { FormatBackendURI } from "Stores/AlertStore";
import type { SilenceFormStore } from "Stores/SilenceFormStore";
import { SilenceComment } from "./SilenceComment";
import { SilenceDetails } from "./SilenceDetails";

const GetAlertmanager = (
  alertStore: AlertStore,
  cluster: string,
): APIAlertmanagerUpstreamT =>
  alertStore.data.readWriteAlertmanagers
    .filter((u) => u.cluster === cluster)
    .slice(0, 1)[0];

const calculatePercent = (startsAt: string, endsAt: string) => {
  const durationDone =
    getUnixTime(new Date()) - getUnixTime(parseISO(startsAt));
  const durationTotal =
    getUnixTime(parseISO(endsAt)) - getUnixTime(parseISO(startsAt));
  return Math.floor((durationDone / durationTotal) * 100);
};

const ManagedSilence: FC<{
  cluster: string;
  alertCount: number;
  alertCountAlwaysVisible: boolean;
  silence: APISilenceT;
  isExpired: boolean;
  alertStore: AlertStore;
  silenceFormStore: SilenceFormStore;
  onEditSilence?: () => boolean;
  isOpen?: boolean;
  onDidUpdate?: () => void;
  isNested?: boolean;
}> = ({
  cluster,
  alertCount,
  alertCountAlwaysVisible,
  silence,
  isExpired,
  alertStore,
  silenceFormStore,
  onEditSilence: externalOnEditSilence,
  isOpen = false,
  onDidUpdate,
  isNested = false,
}) => {
  useEffect(() => {
    if (onDidUpdate) onDidUpdate();
  });

  const [showDetails, setShowDetails] = useState<boolean>(isOpen);
  const effectiveExpired = isExpired;
  const [editPending, setEditPending] = useState(false);
  const editPendingRef = useRef(false);
  const [editError, setEditError] = useState<string | null>(null);
  const mountedRef = useRef(true);

  useEffect(
    () => () => {
      mountedRef.current = false;
    },
    [],
  );

  const onEditSilence = async () => {
    if (externalOnEditSilence && !externalOnEditSilence()) {
      return;
    }
    if (editPendingRef.current) {
      return;
    }
    editPendingRef.current = true;
    const replacementRequest = silenceFormStore.data.beginEditorReplacement();
    const replacementGeneration = replacementRequest.generation;
    setEditPending(true);
    setEditError(null);
    let status: SilenceEditorStatusResponseT;
    try {
      const query = new URLSearchParams({ target: cluster, id: silence.id });
      const response = await fetch(
        FormatBackendURI(`silence-editor/status?${query.toString()}`),
        { ...CommonOptions, method: "GET", signal: replacementRequest.signal },
      );
      if (!response.ok) {
        throw new Error(`failed to verify silence status: ${response.status}`);
      }
      status = (await response.json()) as SilenceEditorStatusResponseT;
      if (
        typeof status.isExpired !== "boolean" ||
        typeof status.member !== "string" ||
        status.member === "" ||
        typeof status.revisionToken !== "string" ||
        status.revisionToken === "" ||
        !status.silence ||
        status.silence.id !== silence.id
      ) {
        throw new Error("failed to verify silence status: invalid response");
      }
    } catch (err) {
      if (
        mountedRef.current &&
        silenceFormStore.data.isEditorReplacementCurrent(replacementGeneration)
      ) {
        silenceFormStore.data.failEditorReplacement(replacementGeneration);
        editPendingRef.current = false;
        setEditPending(false);
        setEditError(
          err instanceof Error
            ? err.message
            : "failed to verify silence status",
        );
      } else if (mountedRef.current) {
        editPendingRef.current = false;
        setEditPending(false);
      }
      return;
    }
    if (
      !silenceFormStore.data.isEditorReplacementCurrent(replacementGeneration)
    ) {
      if (mountedRef.current) {
        editPendingRef.current = false;
        setEditPending(false);
      }
      return;
    }
    if (mountedRef.current) {
      editPendingRef.current = false;
      setEditPending(false);
    }
    runInAction(() => {
      if (
        !silenceFormStore.data.completeEditorReplacement(replacementGeneration)
      ) {
        return;
      }
      const alertmanager = GetAlertmanager(alertStore, cluster);
      silenceFormStore.data.fillFormFromSilence(
        alertmanager,
        status.silence,
        status.isExpired ? "recreate" : "edit",
      );
      silenceFormStore.data.setEditorRouteMember(status.member);
      silenceFormStore.data.setEditorRevisionToken(status.revisionToken);
      silenceFormStore.data.resetProgress();
      silenceFormStore.tab.setTab("editor");
      silenceFormStore.toggle.show();
    });
  };

  const [progress, setProgress] = useState<number>(() =>
    calculatePercent(silence.startsAt, silence.endsAt),
  );

  useEffect(() => {
    const timer = setInterval(() => {
      setProgress(calculatePercent(silence.startsAt, silence.endsAt));
    }, 30 * 1000);
    return () => clearInterval(timer);
  }, [silence.startsAt, silence.endsAt]);

  return (
    <div className="card my-1 components-managed-silence w-100">
      <div className="card-header rounded-0 border-bottom-0 px-2">
        <SilenceComment
          alertStore={alertStore}
          cluster={cluster}
          silence={silence}
          alertCount={alertCount}
          alertCountAlwaysVisible={alertCountAlwaysVisible}
          collapsed={!showDetails}
          collapseToggle={() => setShowDetails(!showDetails)}
        />
      </div>
      {showDetails ? (
        <div className="card-body pt-0 px-2">
          {editError ? (
            <div className="alert alert-danger py-1 px-2" role="alert">
              {editError}
            </div>
          ) : null}
          <SilenceDetails
            cluster={cluster}
            silence={silence}
            isExpired={effectiveExpired}
            alertStore={alertStore}
            silenceFormStore={silenceFormStore}
            onEditSilence={onEditSilence}
            isEditPending={editPending}
            isUpper={isNested}
          />
        </div>
      ) : null}
      <div className="progress silence-progress mx-2 mb-1">
        <div
          className={
            progress > 90
              ? "progress-bar bg-danger"
              : progress > 75
                ? "progress-bar bg-warning"
                : "progress-bar bg-success"
          }
          role="progressbar"
          style={{ width: progress + "%" }}
          aria-valuenow={progress}
          aria-valuemin={0}
          aria-valuemax={100}
        />
      </div>
    </div>
  );
};

export { ManagedSilence, GetAlertmanager };
