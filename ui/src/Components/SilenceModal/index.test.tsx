// Mock react-cool-dimensions to avoid ResizeObserver console.error
jest.mock("react-cool-dimensions", () => ({
  __esModule: true,
  default: () => ({
    observe: jest.fn(),
    unobserve: jest.fn(),
    width: 1000,
    height: 500,
    entry: undefined,
  }),
}));

import { act } from "react";

import { render, fireEvent, screen, waitFor } from "@testing-library/react";

import fetchMock from "@fetch-mock/jest";

import { MockThemeContext } from "__fixtures__/Theme";
import { ThemeContext } from "Components/Theme";
import { AlertStore } from "Stores/AlertStore";
import { Settings } from "Stores/Settings";
import { SilenceFormStore } from "Stores/SilenceFormStore";
import SilenceModal from ".";

let alertStore: AlertStore;
let settingsStore: Settings;
let silenceFormStore: SilenceFormStore;

beforeEach(() => {
  alertStore = new AlertStore([]);
  settingsStore = new Settings(null);
  silenceFormStore = new SilenceFormStore();

  jest.useFakeTimers();
  fetchMock.mockReset();
  fetchMock.route("*", {
    body: JSON.stringify([]),
  });
});

afterEach(() => {
  jest.restoreAllMocks();
  fetchMock.mockReset();
  document.body.className = "";
});

const renderSilenceModal = () => {
  return render(
    <ThemeContext value={MockThemeContext}>
      <SilenceModal
        alertStore={alertStore}
        silenceFormStore={silenceFormStore}
        settingsStore={settingsStore}
      />
    </ThemeContext>,
  );
};

describe("<SilenceModal />", () => {
  it("only renders FontAwesomeIcon when modal is not shown", () => {
    const { container } = renderSilenceModal();
    expect(container.querySelectorAll("svg")).toHaveLength(1);
    expect(container.querySelector(".modal-content")).toBeNull();
  });

  it("renders a spinner placeholder while modal content is loading", () => {
    const { container } = renderSilenceModal();
    const toggle = container.querySelector(".nav-link");
    fireEvent.click(toggle!);
    expect(container.querySelectorAll("svg")).not.toHaveLength(0);
    expect(
      document.body.querySelector(".modal-content svg.fa-spinner"),
    ).toBeInTheDocument();
  });

  it("renders a spinner placeholder after modal content load but no upstreams", () => {
    const { container } = renderSilenceModal();
    const toggle = container.querySelector(".nav-link");
    fireEvent.click(toggle!);
    expect(container.querySelectorAll("svg")).not.toHaveLength(0);
    expect(
      document.body.querySelector(".modal-content svg.fa-spinner"),
    ).toBeInTheDocument();
  });

  it("renders modal content if fallback is not used", () => {
    alertStore.data.setUpstreams({
      counters: { total: 1, healthy: 0, failed: 0 },
      instances: [
        {
          name: "dev",
          cluster: "dev",
          clusterMembers: ["dev"],
          uri: "http://localhost:9093",
          publicURI: "http://localhost:9093",
          error: "",
          warnings: [],
          version: "0.24.0",
          headers: {},
          corsCredentials: "same-origin",
          readonly: false,
        },
      ],
      clusters: { dev: ["dev"] },
    });
    const { container } = renderSilenceModal();
    const toggle = container.querySelector(".nav-link");
    fireEvent.click(toggle!);
    expect(container.querySelectorAll("svg")).not.toHaveLength(0);
    expect(container.querySelector(".modal-content svg.fa-spinner")).toBeNull();
  });

  it("hides the modal when toggle() is called twice", () => {
    const { container } = renderSilenceModal();
    const toggle = container.querySelector(".nav-link");

    fireEvent.click(toggle!);
    act(() => {
      jest.runOnlyPendingTimers();
    });
    expect(document.body.querySelector(".modal-content")).toBeInTheDocument();

    fireEvent.click(toggle!);
    act(() => {
      jest.runOnlyPendingTimers();
    });
    expect(document.body.querySelector(".modal-content")).toBeNull();
  });

  it("hides the modal when hide() is called", () => {
    // Verifies modal closes when hide() is called on the toggle store
    const { container } = renderSilenceModal();
    const toggle = container.querySelector(".nav-link");

    fireEvent.click(toggle!);
    act(() => {
      jest.runOnlyPendingTimers();
    });
    expect(document.body.querySelector(".modal-content")).toBeInTheDocument();

    act(() => {
      silenceFormStore.toggle.hide();
    });
    act(() => {
      jest.runOnlyPendingTimers();
    });
    expect(document.body.querySelector(".modal-content")).toBeNull();
  });

  it("resets progress on hide", () => {
    const { container } = renderSilenceModal();
    const toggle = container.querySelector(".nav-link");
    fireEvent.click(toggle!);

    // mark form as dirty, resetProgress() should change this value to false
    silenceFormStore.data.setWasValidated(true);
    // disable autofill, closing modal should re-enable it
    silenceFormStore.data.setAutofillMatchers(false);

    // click to hide
    fireEvent.click(toggle!);
    // wait for animation to finish
    act(() => {
      jest.runOnlyPendingTimers();
    });
    // form should be reset
    expect(silenceFormStore.data.currentStage).toBe("form");
    expect(silenceFormStore.data.wasValidated).toBe(false);
    expect(silenceFormStore.data.autofillMatchers).toBe(true);
  });

  it("'modal-open' class is appended to body node when modal is visible", () => {
    const { container } = renderSilenceModal();
    const toggle = container.querySelector(".nav-link");
    fireEvent.click(toggle!);
    expect(document.body.className.split(" ")).toContain("modal-open");
  });

  it("'modal-open' class is removed from body node after modal is hidden", () => {
    const { container } = renderSilenceModal();
    const toggle = container.querySelector(".nav-link");

    fireEvent.click(toggle!);
    expect(document.body.className.split(" ")).toContain("modal-open");

    fireEvent.click(toggle!);
    act(() => {
      jest.runOnlyPendingTimers();
    });
    expect(document.body.className.split(" ")).not.toContain("modal-open");
  });

  it("'modal-open' class is removed from body node after modal is unmounted", () => {
    const { container, unmount } = renderSilenceModal();
    const toggle = container.querySelector(".nav-link");
    fireEvent.click(toggle!);
    unmount();
    expect(document.body.className.split(" ")).not.toContain("modal-open");
  });

  it("routes header close and Escape through the editor dirty guard", async () => {
    alertStore.data.setUpstreams({
      counters: { total: 1, healthy: 1, failed: 0 },
      instances: [
        {
          name: "dev-member",
          cluster: "dev",
          clusterMembers: ["dev-member"],
          uri: "http://localhost:9093",
          publicURI: "http://localhost:9093",
          error: "",
          warnings: [],
          version: "0.24.0",
          headers: {},
          corsCredentials: "same-origin",
          readonly: false,
        },
      ],
      clusters: { dev: ["dev-member"] },
    });
    silenceFormStore.data.setAutofillMatchers(false);
    silenceFormStore.data.addEmptyMatcher();
    silenceFormStore.data.matchers[0].name = "alertname";
    silenceFormStore.data.matchers[0].values = [
      { label: "HostDown", value: "HostDown", wasCreated: false },
    ];
    const confirmSpy = jest.spyOn(window, "confirm").mockReturnValue(false);

    const { container } = renderSilenceModal();
    fireEvent.click(container.querySelector(".nav-link")!);
    const commentField = await waitFor(() => {
      const field = screen
        .getByText("Comment")
        .parentElement?.querySelector("textarea");
      if (!field) {
        throw new Error("comment textarea not ready");
      }
      return field as HTMLTextAreaElement;
    });
    fireEvent.change(commentField, {
      target: { value: "changed" },
    });

    const closeButtons = document.body.querySelectorAll(
      ".modal-content .btn-close",
    );
    fireEvent.click(closeButtons[0]);
    expect(document.body.querySelector(".modal-content")).toBeInTheDocument();

    fireEvent.keyDown(document, { key: "Escape", code: "Escape" });
    expect(document.body.querySelector(".modal-content")).toBeInTheDocument();
    expect(confirmSpy).toHaveBeenCalledTimes(2);
  });

  it("always opens editor tab from navbar toggle", async () => {
    alertStore.data.setUpstreams({
      counters: { total: 1, healthy: 1, failed: 0 },
      instances: [
        {
          name: "dev-member",
          cluster: "dev",
          clusterMembers: ["dev-member"],
          uri: "http://localhost:9093",
          publicURI: "http://localhost:9093",
          error: "",
          warnings: [],
          version: "0.24.0",
          headers: {},
          corsCredentials: "same-origin",
          readonly: false,
        },
      ],
      clusters: { dev: ["dev-member"] },
    });

    const { container } = renderSilenceModal();
    const toggle = container.querySelector(".nav-link");
    fireEvent.click(toggle!);
    await waitFor(() =>
      expect(
        screen.getByRole("button", { name: "Create silence" }),
      ).toBeInTheDocument(),
    );

    fireEvent.click(toggle!);
    act(() => {
      jest.runOnlyPendingTimers();
    });
    fireEvent.click(toggle!);

    await waitFor(() =>
      expect(
        screen.getByRole("button", { name: "Create silence" }),
      ).toBeInTheDocument(),
    );
    expect(screen.queryByText("Browse")).not.toBeInTheDocument();
  });

  it("blocks navbar close when registered guard rejects close", async () => {
    alertStore.data.setUpstreams({
      counters: { total: 1, healthy: 1, failed: 0 },
      instances: [
        {
          name: "dev-member",
          cluster: "dev",
          clusterMembers: ["dev-member"],
          uri: "http://localhost:9093",
          publicURI: "http://localhost:9093",
          error: "",
          warnings: [],
          version: "0.24.0",
          headers: {},
          corsCredentials: "same-origin",
          readonly: false,
        },
      ],
      clusters: { dev: ["dev-member"] },
    });

    const confirmSpy = jest.spyOn(window, "confirm").mockReturnValue(false);
    const { container } = renderSilenceModal();
    const toggle = container.querySelector(".nav-link");
    fireEvent.click(toggle!);

    const commentField = await waitFor(() => {
      const field = screen
        .getByText("Comment")
        .parentElement?.querySelector("textarea");
      if (!field) {
        throw new Error("comment textarea not ready");
      }
      return field as HTMLTextAreaElement;
    });
    fireEvent.change(commentField, {
      target: { value: "changed" },
    });

    fireEvent.click(toggle!);
    expect(document.body.querySelector(".modal-content")).toBeInTheDocument();
    expect(confirmSpy).toHaveBeenCalled();
  });
});
