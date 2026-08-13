import { FC, memo } from "react";

import type { APIManagedSilenceT } from "Models/APITypes";
import type { AlertStore } from "Stores/AlertStore";
import type { SilenceFormStore } from "Stores/SilenceFormStore";
import { ManagedSilence } from "Components/ManagedSilence";

const FallbackSilenceDescription: FC<{
  silenceID: string;
}> = ({ silenceID }) => {
  return (
    <div className="m-1">
      <small className="text-muted">Silenced by {silenceID}</small>
    </div>
  );
};

const GetSilenceFromStore = (
  alertStore: AlertStore,
  cluster: string,
  silenceID: string,
): APIManagedSilenceT | null => {
  const amSilences = alertStore.data.silences[cluster];
  if (!amSilences) return null;

  // next check if alertmanager has our silence ID
  const found = amSilences[silenceID];
  if (!found) return null;

  return {
    cluster,
    alertCount: 0,
    isExpired: found.isExpired,
    silence: found,
  };
};

const RenderSilence: FC<{
  alertStore: AlertStore;
  silenceFormStore: SilenceFormStore;
  afterUpdate: () => void;
  cluster: string;
  silenceID: string;
}> = memo(
  ({ alertStore, silenceFormStore, afterUpdate, cluster, silenceID }) => {
    const silence = GetSilenceFromStore(alertStore, cluster, silenceID);

    if (silence === null) {
      return (
        <FallbackSilenceDescription
          key={silenceID}
          silenceID={silenceID}
        ></FallbackSilenceDescription>
      );
    }

    return (
      <ManagedSilence
        key={silenceID}
        cluster={cluster}
        alertCount={0}
        alertCountAlwaysVisible={false}
        silence={silence.silence}
        isExpired={silence.isExpired}
        alertStore={alertStore}
        silenceFormStore={silenceFormStore}
        onDidUpdate={afterUpdate}
      />
    );
  },
  (prevProps, nextProps) => {
    if (
      prevProps.cluster === nextProps.cluster &&
      prevProps.silenceID === nextProps.silenceID
    ) {
      return (
        GetSilenceFromStore(
          nextProps.alertStore,
          nextProps.cluster,
          nextProps.silenceID,
        ) === null
      );
    }
    return false;
  },
);

export { RenderSilence };
