import React from "react";
import { act } from "react";

import { fireEvent, render, screen } from "@testing-library/react";

import { PressKey } from "__fixtures__/PressKey";
import {
  MockThemeContext,
  MockThemeContextWithoutAnimations,
} from "__fixtures__/Theme";
import { ThemeContext } from "Components/Theme";
import { Modal, ModalInner } from ".";

beforeEach(() => {
  jest.restoreAllMocks();
  fakeToggle.mockClear();
});

afterEach(() => {
  jest.restoreAllMocks();
  document.body.className = "";
});

const fakeToggle = jest.fn();

const renderModal = (isOpen: boolean, isUpper?: boolean) => {
  return render(
    <Modal isOpen={isOpen} isUpper={isUpper || false} toggleOpen={fakeToggle}>
      <div data-testid="modal-child" />
    </Modal>,
  );
};

describe("<ModalInner />", () => {
  it("exposes an accessible dialog name", () => {
    render(
      <Modal ariaLabel="Silence editor" isOpen toggleOpen={jest.fn()}>
        <button type="button">Inside</button>
      </Modal>,
    );
    expect(
      screen.getByRole("dialog", { name: "Silence editor" }),
    ).toBeVisible();
  });

  it("does not let an outer focus trap handle an upper portal event", () => {
    render(
      <Modal ariaLabel="Outer" isOpen toggleOpen={jest.fn()}>
        <button type="button">Outer action</button>
        <Modal ariaLabel="Inner" isOpen isUpper toggleOpen={jest.fn()}>
          <button type="button">Inner action</button>
        </Modal>
      </Modal>,
    );
    const innerAction = screen.getByRole("button", { name: "Inner action" });
    innerAction.focus();
    fireEvent.keyDown(innerAction, { key: "Tab" });
    expect(innerAction).toHaveFocus();
  });

  it("'modal-open' class is appended to MountModal container", () => {
    renderModal(true);
    expect(document.body.querySelector("div.modal-open")).toBeInTheDocument();
  });

  it("'modal-open' class is appended to body node when modal is visible", () => {
    renderModal(true);
    expect(document.body.className.split(" ")).toContain("modal-open");
  });

  it("'modal-open' class is not removed from body node after hidden modal is unmounted", () => {
    document.body.classList.add("modal-open");
    const { unmount } = renderModal(false);
    unmount();
    expect(document.body.className.split(" ")).toContain("modal-open");
  });

  it("'modal-open' class is removed from body node after modal is unmounted", () => {
    const { unmount } = renderModal(true);
    act(() => {
      unmount();
    });
    expect(document.body.className.split(" ")).not.toContain("modal-open");
  });

  it("'modal-open' class is not removed from body when hidden modal is updated", () => {
    document.body.classList.toggle("modal-open", true);
    const { rerender } = renderModal(false);
    expect(document.body.className.split(" ")).toContain("modal-open");
    rerender(
      <Modal isOpen={false} isUpper={false} toggleOpen={fakeToggle}>
        <div data-testid="modal-child" />
      </Modal>,
    );
    expect(document.body.className.split(" ")).toContain("modal-open");
  });

  it("'modal-open' class is removed from body when visible modal is updated to be hidden", () => {
    document.body.classList.toggle("modal-open", true);
    const { rerender } = renderModal(true);
    expect(document.body.className.split(" ")).toContain("modal-open");

    rerender(
      <Modal isOpen={false} isUpper={false} toggleOpen={fakeToggle}>
        <div data-testid="modal-child" />
      </Modal>,
    );
    expect(document.body.className.split(" ")).toContain("modal-open");
  });

  it("'modal-open' class is not removed if Modal isUpper=true and is unmounted", () => {
    const { unmount } = renderModal(true, true);
    expect(document.body.className.split(" ")).toContain("modal-open");

    act(() => {
      unmount();
    });
    expect(document.body.className.split(" ")).toContain("modal-open");
  });

  it("'modal-open' class is not removed if Modal isUpper=true and is updated to be hidden", () => {
    const { rerender } = renderModal(true, true);
    expect(document.body.className.split(" ")).toContain("modal-open");

    rerender(
      <Modal isOpen={false} isUpper={true} toggleOpen={fakeToggle}>
        <div data-testid="modal-child" />
      </Modal>,
    );
    expect(document.body.className.split(" ")).toContain("modal-open");
  });

  it("passes extra props down to the CSSTransition animation component", () => {
    const onExited = jest.fn();
    render(
      <Modal isOpen={true} toggleOpen={fakeToggle} onExited={onExited}>
        <div />
      </Modal>,
    );
    expect(document.body.querySelector(".modal")).toBeInTheDocument();
  });

  it("uses components-animation-modal class when animations are enabled", () => {
    const onExited = jest.fn();
    render(
      <ThemeContext value={MockThemeContext}>
        <Modal isOpen={true} toggleOpen={fakeToggle} onExited={onExited}>
          <div />
        </Modal>
      </ThemeContext>,
    );
    expect(document.body.querySelector(".modal")).toBeInTheDocument();
  });

  it("doesn't use components-animation-modal class when animations are disabled", () => {
    const onExited = jest.fn();
    render(
      <ThemeContext value={MockThemeContextWithoutAnimations}>
        <Modal isOpen={true} toggleOpen={fakeToggle} onExited={onExited}>
          <div />
        </Modal>
      </ThemeContext>,
    );
    expect(document.body.querySelector(".modal")).toBeInTheDocument();
  });

  it("toggleOpen is called after pressing 'esc'", () => {
    renderModal(true);
    PressKey("Escape", 27);
    expect(fakeToggle).toHaveBeenCalled();
  });

  it("handles Escape while focus is in a form control", () => {
    render(
      <Modal isOpen toggleOpen={fakeToggle}>
        <input data-modal-initial-focus aria-label="Editor field" />
      </Modal>,
    );
    expect(screen.getByLabelText("Editor field")).toHaveFocus();
    PressKey("Escape", 27);
    expect(fakeToggle).toHaveBeenCalledTimes(1);
  });

  it("focuses the preferred control and restores the invoking control", () => {
    const trigger = document.createElement("button");
    document.body.appendChild(trigger);
    trigger.focus();

    const { unmount } = render(
      <Modal isOpen toggleOpen={fakeToggle}>
        <button type="button">First</button>
        <input data-modal-initial-focus aria-label="Preferred" />
      </Modal>,
    );

    expect(screen.getByLabelText("Preferred")).toHaveFocus();
    unmount();
    expect(trigger).toHaveFocus();
    trigger.remove();
  });

  it("focuses a replacement subtree when the active control is removed", async () => {
    const { rerender } = render(
      <Modal isOpen toggleOpen={fakeToggle}>
        <button type="button">Browse edit</button>
      </Modal>,
    );
    const browseEdit = screen.getByRole("button", { name: "Browse edit" });
    browseEdit.focus();
    rerender(
      <Modal isOpen toggleOpen={fakeToggle}>
        <input data-modal-initial-focus aria-label="Replacement editor" />
      </Modal>,
    );
    await act(async () => {});
    expect(screen.getByLabelText("Replacement editor")).toHaveFocus();
  });

  it("focuses lazy content after the dialog itself received fallback focus", async () => {
    const { rerender } = render(
      <Modal isOpen toggleOpen={fakeToggle}>
        <div>Loading</div>
      </Modal>,
    );
    const dialog = screen.getByRole("dialog");
    expect(dialog).toHaveFocus();

    rerender(
      <Modal isOpen toggleOpen={fakeToggle}>
        <input data-modal-initial-focus aria-label="Lazy editor field" />
      </Modal>,
    );
    await act(async () => {});

    expect(screen.getByLabelText("Lazy editor field")).toHaveFocus();
  });

  it("traps Tab and Shift+Tab inside the dialog", () => {
    render(
      <Modal isOpen toggleOpen={fakeToggle}>
        <button type="button">First</button>
        <button type="button">Last</button>
      </Modal>,
    );
    const first = screen.getByRole("button", { name: "First" });
    const last = screen.getByRole("button", { name: "Last" });

    last.focus();
    fireEvent.keyDown(last, { key: "Tab" });
    expect(first).toHaveFocus();

    first.focus();
    fireEvent.keyDown(first, { key: "Tab", shiftKey: true });
    expect(last).toHaveFocus();
  });

  it("scroll isn't enabled if ref is null", () => {
    const useRefSpy = jest.spyOn(React, "useRef").mockImplementation(() =>
      Object.defineProperty({} as { current: unknown }, "current", {
        get: () => null,
        set: () => {},
      }),
    );
    const { rerender } = render(
      <ModalInner size="modal-lg" isUpper toggleOpen={fakeToggle}>
        <div>test</div>
      </ModalInner>,
    );
    rerender(
      <ModalInner size="modal-lg" isUpper={false} toggleOpen={fakeToggle}>
        <div>test</div>
      </ModalInner>,
    );
    rerender(
      <ModalInner size="modal-lg" isUpper={true} toggleOpen={fakeToggle}>
        <div>test</div>
      </ModalInner>,
    );
    rerender(
      <ModalInner size="modal-lg" isUpper={false} toggleOpen={fakeToggle}>
        <div>test</div>
      </ModalInner>,
    );
    expect(useRefSpy).toHaveBeenCalled();
    expect(document.body.className.split(" ")).not.toContain("modal-open");
  });
});
