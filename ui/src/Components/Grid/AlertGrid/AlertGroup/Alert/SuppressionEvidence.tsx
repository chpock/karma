import type { FC } from "react";

import type { APIAlertSuppressionCauseT, APISilenceT } from "Models/APITypes";
import type { AlertStore } from "Stores/AlertStore";
import type { SilenceFormStore } from "Stores/SilenceFormStore";
import { ManagedSilence } from "Components/ManagedSilence";
import { RenderSilence } from "../Silences";

const causeToSilence = (
  cause: APIAlertSuppressionCauseT,
): APISilenceT | null => {
  if (
    cause.resolutionStatus !== "known" ||
    cause.startsAt === null ||
    cause.endsAt === null
  ) {
    return null;
  }

  return {
    id: cause.silenceID,
    matchers: cause.matchers,
    startsAt: cause.startsAt,
    endsAt: cause.endsAt,
    isExpired: new Date(cause.endsAt).getTime() <= Date.now(),
    createdAt: "",
    createdBy: cause.createdBy,
    comment: cause.comment,
    ticketID: "",
    ticketURL: "",
  };
};

const SuppressionEvidence: FC<{
  causes: APIAlertSuppressionCauseT[];
  alertStore: AlertStore;
  silenceFormStore: SilenceFormStore;
  afterUpdate: () => void;
}> = ({ causes, alertStore, silenceFormStore, afterUpdate }) => {
  if (causes.length === 0) return null;

  return causes.map((cause) => {
    const silence = causeToSilence(cause);
    if (silence === null) {
      // This follows the live rule fallback: without a complete snapshot, show a
      // current rule if one is available and otherwise only its known ID.
      return (
        <RenderSilence
          key={`${cause.target}/${cause.source}/${cause.silenceID}`}
          alertStore={alertStore}
          silenceFormStore={silenceFormStore}
          afterUpdate={afterUpdate}
          cluster={cause.target}
          silenceID={cause.silenceID}
        />
      );
    }

    return (
      <ManagedSilence
        key={`${cause.target}/${cause.source}/${cause.silenceID}`}
        cluster={cause.target}
        alertCount={0}
        alertCountAlwaysVisible={false}
        silence={silence}
        isExpired={silence.isExpired}
        alertStore={alertStore}
        silenceFormStore={silenceFormStore}
        onDidUpdate={afterUpdate}
      />
    );
  });
};

export { SuppressionEvidence };
