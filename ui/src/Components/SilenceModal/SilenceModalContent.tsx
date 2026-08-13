import type { FC } from "react";

import { Observer } from "mobx-react-lite";

import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { faSpinner } from "@fortawesome/free-solid-svg-icons/faSpinner";
import { faLock } from "@fortawesome/free-solid-svg-icons/faLock";

import type { AlertStore } from "Stores/AlertStore";
import type { SilenceFormStore } from "Stores/SilenceFormStore";
import type { Settings } from "Stores/Settings";
import type { SilenceEditorCloseReasonT } from "./index";
import SilenceEditor from "./SilenceEditor";

const ReadOnlyPlaceholder: FC = () => (
  <div className="px-2 py-5 bg-transparent">
    <h1 className="display-5 text-placeholder text-center">
      <FontAwesomeIcon icon={faLock} className="me-3" />
      Read only mode
    </h1>
  </div>
);

const SilenceModalContent: FC<{
  alertStore: AlertStore;
  silenceFormStore: SilenceFormStore;
  settingsStore: Settings;
  onHide: () => void;
  onSubmitted: (target: string, silenceID: string) => void;
  registerCloseGuard: (
    guard: (reason: SilenceEditorCloseReasonT) => boolean,
  ) => void;
}> = ({
  alertStore,
  silenceFormStore,
  settingsStore,
  onHide,
  onSubmitted,
  registerCloseGuard,
}) => {
  return (
    <Observer>
      {() => {
        const editorKey = `${silenceFormStore.data.operation}:${silenceFormStore.data.silenceID ?? "new"}:${silenceFormStore.data.editorReplacementRevision}`;

        return (
          <>
            <div className="modal-header py-2">
              <h5 className="modal-title">
                {silenceFormStore.data.silenceID === null
                  ? "Create silence"
                  : silenceFormStore.data.operation === "recreate"
                    ? "Recreate silence"
                    : "Edit silence"}
              </h5>
              <button
                type="button"
                className="btn-close"
                aria-label="Close silence editor"
                onClick={onHide}
              ></button>
            </div>
            <div
              className={`modal-body ${
                silenceFormStore.toggle.blurred ? "modal-content-blur" : ""
              }`}
            >
              {alertStore.data.upstreams.instances.length > 0 ? (
                Object.keys(alertStore.data.clustersWithoutReadOnly).length >
                0 ? (
                  <div>
                    <SilenceEditor
                      key={editorKey}
                      alertStore={alertStore}
                      silenceFormStore={silenceFormStore}
                      settingsStore={settingsStore}
                      onClose={onHide}
                      onSubmitted={onSubmitted}
                      registerCloseGuard={registerCloseGuard}
                    />
                  </div>
                ) : (
                  <ReadOnlyPlaceholder />
                )
              ) : (
                <h1 className="text-center display-1 text-placeholder p-5 m-auto">
                  <FontAwesomeIcon icon={faSpinner} size="lg" spin />
                </h1>
              )}
            </div>
          </>
        );
      }}
    </Observer>
  );
};

export { SilenceModalContent };
