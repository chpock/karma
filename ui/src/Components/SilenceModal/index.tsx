import React, { FC, useCallback, useRef, useState } from "react";

import { observer } from "mobx-react-lite";

import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { faBellSlash } from "@fortawesome/free-solid-svg-icons/faBellSlash";
import { faCheck } from "@fortawesome/free-solid-svg-icons/faCheck";
import { faSpinner } from "@fortawesome/free-solid-svg-icons/faSpinner";

import type { AlertStore } from "Stores/AlertStore";
import type { SilenceFormStore } from "Stores/SilenceFormStore";
import type { Settings } from "Stores/Settings";
import { Modal } from "Components/Modal";
import { TooltipWrapper } from "Components/TooltipWrapper";
import { Toast, ToastContainer } from "Components/Toast";
import { ToastMessage } from "Components/Toast/ToastMessages";

export type SilenceEditorCloseReasonT = "close" | "replace";

// https://github.com/facebook/react/issues/14603
const SilenceModalContent = React.lazy(() =>
  import("./SilenceModalContent").then((module) => ({
    default: module.SilenceModalContent,
  })),
);

const SilenceModal: FC<{
  alertStore: AlertStore;
  silenceFormStore: SilenceFormStore;
  settingsStore: Settings;
}> = ({ alertStore, silenceFormStore, settingsStore }) => {
  const [success, setSuccess] = useState<{
    target: string;
    silenceID: string;
    nonce: number;
  } | null>(null);
  const successCounterRef = useRef(0);
  const closeGuardRef = useRef<(reason: SilenceEditorCloseReasonT) => boolean>(
    () => true,
  );
  const registerCloseGuard = useCallback(
    (guard: (reason: SilenceEditorCloseReasonT) => boolean) => {
      closeGuardRef.current = guard;
    },
    [],
  );
  const canNavigate = useCallback(
    (reason: SilenceEditorCloseReasonT) => closeGuardRef.current(reason),
    [],
  );
  const requestClose = useCallback(() => {
    if (canNavigate("close")) {
      silenceFormStore.data.cancelEditorReplacement();
      silenceFormStore.data.transitionEditorSession("CLOSE");
      silenceFormStore.toggle.hide();
    }
  }, [canNavigate, silenceFormStore]);
  const toggleModal = useCallback(() => {
    if (silenceFormStore.toggle.visible) {
      requestClose();
    } else {
      setSuccess(null);
      silenceFormStore.data.transitionEditorSession("BEGIN_CREATE");
      silenceFormStore.tab.setTab("editor");
      silenceFormStore.toggle.show();
    }
  }, [requestClose, silenceFormStore]);

  return (
    <>
      <li
        className={`nav-item components-navbar-button ${
          silenceFormStore.toggle.visible ? "border-info" : ""
        } ml-auto`}
      >
        <TooltipWrapper title="New silence">
          <button
            type="button"
            id="components-new-silence"
            className="nav-link cursor-pointer border-0 bg-transparent"
            aria-label="New silence"
            onClick={toggleModal}
          >
            <FontAwesomeIcon icon={faBellSlash} className="fa-fw" />
          </button>
        </TooltipWrapper>
      </li>
      <Modal
        ariaLabel="Silence editor"
        size="modal-xl"
        isOpen={silenceFormStore.toggle.visible}
        toggleOpen={requestClose}
        onExited={() => {
          closeGuardRef.current = () => true;
          silenceFormStore.data.cancelEditorReplacement();
          silenceFormStore.data.transitionEditorSession("CLOSE");
          silenceFormStore.data.resetProgress();
          silenceFormStore.data.setAutofillMatchers(true);
          silenceFormStore.data.setSilenceID(null);
        }}
      >
        <React.Suspense
          fallback={
            <h1 className="display-1 text-placeholder p-5 m-auto">
              <FontAwesomeIcon icon={faSpinner} size="lg" spin />
            </h1>
          }
        >
          <SilenceModalContent
            alertStore={alertStore}
            silenceFormStore={silenceFormStore}
            settingsStore={settingsStore}
            onHide={requestClose}
            onSubmitted={(target, silenceID) => {
              successCounterRef.current++;
              setSuccess({
                target,
                silenceID,
                nonce: successCounterRef.current,
              });
            }}
            registerCloseGuard={registerCloseGuard}
          />
        </React.Suspense>
      </Modal>
      {success ? (
        <ToastContainer key={success.nonce}>
          <Toast
            icon={faCheck}
            iconClass="text-success"
            message={
              <ToastMessage
                title={`Silence submitted to ${success.target}`}
                message={`Silence ID: ${success.silenceID}`}
              />
            }
            hasClose
          />
        </ToastContainer>
      ) : null}
    </>
  );
};

export default observer(SilenceModal);
