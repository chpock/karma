import React, { use, FC, useEffect, useRef } from "react";
import ReactDOM from "react-dom";

import { CSSTransition } from "react-transition-group";

import { disableBodyScroll, enableBodyScroll } from "body-scroll-lock";

import { useHotkeys } from "react-hotkeys-hook";

import { ThemeContext } from "Components/Theme";

const ModalInner: FC<{
  size: "modal-lg" | "modal-xl";
  isUpper: boolean;
  toggleOpen: () => void;
  ariaLabel?: string;
  children: React.ReactNode;
}> = ({ size, isUpper, toggleOpen, ariaLabel, children }) => {
  // needed for tests to spy on useRef
  const ref = React.useRef<HTMLDivElement | null>(null);
  const previouslyFocusedRef = React.useRef<HTMLElement | null>(null);

  const getFocusableElements = (modal: HTMLElement): HTMLElement[] =>
    Array.from(
      modal.querySelectorAll<HTMLElement>(
        'button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), a[href], [tabindex]:not([tabindex="-1"])',
      ),
    ).filter(
      (element) =>
        !element.closest("[hidden], .d-none") &&
        element.getAttribute("aria-hidden") !== "true",
    );

  useEffect(() => {
    if (ref.current !== null) {
      document.body.classList.add("modal-open");
      disableBodyScroll(ref.current, { reserveScrollBarGap: true });

      const modal = ref.current;
      previouslyFocusedRef.current =
        document.activeElement instanceof HTMLElement
          ? document.activeElement
          : null;
      const focusInitialControl = () => {
        const preferred = modal.querySelector<HTMLElement>(
          "[data-modal-initial-focus]",
        );
        const target = preferred || getFocusableElements(modal)[0];
        if (target) {
          target.focus();
          return true;
        }
        modal.focus();
        return false;
      };
      focusInitialControl();
      const observer = new MutationObserver(() => {
        const active = document.activeElement;
        if (
          active === null ||
          active === document.body ||
          active === modal ||
          !active.isConnected ||
          (active instanceof Node && !modal.contains(active))
        ) {
          focusInitialControl();
        }
      });
      observer.observe(modal, { childList: true, subtree: true });
      return () => {
        observer?.disconnect();
        if (!isUpper) document.body.classList.remove("modal-open");
        enableBodyScroll(modal);
        if (previouslyFocusedRef.current?.isConnected) {
          previouslyFocusedRef.current.focus();
        }
      };
    }
  }, [isUpper]);

  useHotkeys("esc", toggleOpen, {
    enableOnFormTags: true,
    enableOnContentEditable: true,
  });

  const trapFocus = (event: React.KeyboardEvent<HTMLDivElement>) => {
    if (
      event.key !== "Tab" ||
      ref.current === null ||
      !(event.target instanceof Node) ||
      !ref.current.contains(event.target)
    ) {
      return;
    }
    const focusable = getFocusableElements(ref.current);
    if (focusable.length === 0) {
      event.preventDefault();
      ref.current.focus();
      return;
    }
    const first = focusable[0];
    const last = focusable[focusable.length - 1];
    if (event.shiftKey && document.activeElement === first) {
      event.preventDefault();
      last.focus();
    } else if (
      !event.shiftKey &&
      (document.activeElement === last ||
        !ref.current.contains(document.activeElement))
    ) {
      event.preventDefault();
      first.focus();
    }
  };

  return (
    <div className="modal-open">
      <div
        ref={ref}
        className="modal d-block"
        role="dialog"
        aria-modal="true"
        aria-label={ariaLabel}
        tabIndex={-1}
        onKeyDown={trapFocus}
      >
        <div
          className={`modal-dialog ${size} ${
            isUpper ? "modal-upper shadow" : ""
          }`}
          role="document"
        >
          <div className="modal-content">{children}</div>
        </div>
      </div>
    </div>
  );
};

const Modal: FC<{
  size?: "modal-lg" | "modal-xl";
  isOpen: boolean;
  isUpper?: boolean;
  toggleOpen: () => void;
  onExited?: () => void;
  ariaLabel?: string;
  children: React.ReactNode;
}> = ({
  size = "modal-lg",
  isOpen,
  isUpper = false,
  toggleOpen,
  onExited,
  ariaLabel,
  children,
}) => {
  const context = use(ThemeContext);
  const modalRef = useRef<HTMLDivElement>(null);
  const backdropRef = useRef<HTMLDivElement>(null);

  return ReactDOM.createPortal(
    <>
      <CSSTransition
        in={isOpen}
        classNames={
          context.animations.duration ? "components-animation-modal" : ""
        }
        timeout={context.animations.duration ? 300 : 0}
        onExited={onExited}
        enter
        exit
        unmountOnExit
        nodeRef={modalRef}
      >
        <div ref={modalRef}>
          <ModalInner
            size={size}
            isUpper={isUpper}
            toggleOpen={toggleOpen}
            ariaLabel={ariaLabel}
          >
            {children}
          </ModalInner>
        </div>
      </CSSTransition>
      <CSSTransition
        in={isOpen && !isUpper}
        classNames="components-animation-backdrop"
        timeout={context.animations.duration ? 300 : 0}
        enter
        exit
        unmountOnExit
        nodeRef={backdropRef}
      >
        <div ref={backdropRef} className="modal-backdrop d-block" />
      </CSSTransition>
    </>,
    document.body,
  );
};

export { Modal, ModalInner };
