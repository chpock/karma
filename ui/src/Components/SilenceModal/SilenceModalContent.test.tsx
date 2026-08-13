import { act, render, fireEvent, screen, waitFor } from "@testing-library/react";

import { MockThemeContext } from "__fixtures__/Theme";
import { AlertStore } from "Stores/AlertStore";
import { Settings } from "Stores/Settings";
import { SilenceFormStore } from "Stores/SilenceFormStore";
import { ThemeContext } from "Components/Theme";
import { SilenceModalContent } from "./SilenceModalContent";
import type { APIAlertsResponseUpstreamsT } from "Models/APITypes";

let alertStore: AlertStore;
let settingsStore: Settings;
let silenceFormStore: SilenceFormStore;

const generateUpstreams = (): APIAlertsResponseUpstreamsT => ({
  counters: { total: 1, healthy: 1, failed: 0 },
  instances: [
    {
      name: "am1",
      cluster: "am",
      clusterMembers: ["am1"],
      uri: "http://localhost:9093",
      publicURI: "http://localhost:9093",
      readonly: false,
      error: "",
      warnings: [],
      version: "0.24.0",
      headers: {},
      corsCredentials: "include",
    },
  ],
  clusters: { am: ["am1"] },
});

beforeEach(() => {
  alertStore = new AlertStore([]);
  settingsStore = new Settings(null);
  silenceFormStore = new SilenceFormStore();

  alertStore.data.setUpstreams(generateUpstreams());

  silenceFormStore.tab.setTab("editor");
});

afterEach(() => {
  jest.restoreAllMocks();
});

const MockOnHide = jest.fn();

const renderSilenceModalContent = () => {
  return render(
    <ThemeContext value={MockThemeContext}>
      <SilenceModalContent
        alertStore={alertStore}
        settingsStore={settingsStore}
        silenceFormStore={silenceFormStore}
        onHide={MockOnHide}
        onSubmitted={() => {}}
        registerCloseGuard={() => {}}
      />
    </ThemeContext>,
  );
};

describe("<SilenceModalContent />", () => {
  it("Renders ReadOnlyPlaceholder when there are no writable Alertmanager upstreams", () => {
    const upstreams = generateUpstreams();
    upstreams.instances[0].readonly = true;
    alertStore.data.setUpstreams(upstreams);
    const { container } = renderSilenceModalContent();
    expect(container.innerHTML).toMatch(/Read only mode/);
  });

  it("never exposes Browse and ignores a stale browser tab value", () => {
    silenceFormStore.tab.setTab("browser");
    const { container } = renderSilenceModalContent();
    expect(container).not.toHaveTextContent("Browse");
    expect(container.querySelector(".fork-silence-editor")).toBeInTheDocument();
    expect(container.innerHTML).not.toMatch(/Sort order/);
  });

  it("gives the icon-only close button an accessible name", () => {
    renderSilenceModalContent();
    expect(
      screen.getAllByRole("button", { name: "Close silence editor" }),
    ).toHaveLength(1);
  });

  it("Content is not blurred when silenceFormStore.toggle.blurred is false", () => {
    silenceFormStore.toggle.setBlur(false);
    const { container } = renderSilenceModalContent();
    expect(
      container.querySelector("div.modal-body.modal-content-blur"),
    ).toBeNull();
  });

  it("Content is blurred when silenceFormStore.toggle.blurred is true", () => {
    silenceFormStore.toggle.setBlur(true);
    const { container } = renderSilenceModalContent();
    expect(
      container.querySelector("div.modal-body.modal-content-blur"),
    ).toBeInTheDocument();
  });
});

describe("<SilenceModalContent /> Editor", () => {
  it("has one 'Create silence' title when creating a new silence", () => {
    silenceFormStore.data.setSilenceID(null);
    renderSilenceModalContent();
    expect(
      screen.getAllByRole("heading", { name: "Create silence" }),
    ).toHaveLength(1);
  });

  it("has one modal header", () => {
    const { container } = renderSilenceModalContent();
    expect(container.querySelectorAll(".modal-header")).toHaveLength(1);
    expect(
      container.querySelectorAll(".fork-silence-editor__header"),
    ).toHaveLength(0);
  });

  it("title is 'Edit silence' when editing an existing silence", () => {
    silenceFormStore.data.setSilenceID("1234");
    renderSilenceModalContent();
    expect(screen.getByRole("heading", { name: "Edit silence" })).toBeVisible();
  });

  it("title is 'Recreate silence' when recreating an expired silence", () => {
    silenceFormStore.data.setSilenceID("1234");
    silenceFormStore.data.setOperation("recreate");
    renderSilenceModalContent();
    expect(
      screen.getByRole("heading", { name: "Recreate silence" }),
    ).toBeVisible();
  });

  it("renders new compact silence editor", () => {
    const { container } = renderSilenceModalContent();
    expect(container.innerHTML).toMatch(/Affected alerts/);
    expect(container.querySelector(".fork-silence-editor")).toBeInTheDocument();
  });

  it("remounts editor state when an external edit selects another silence", async () => {
    const { container, getByDisplayValue } = renderSilenceModalContent();
    const authorInput = container.querySelector(
      ".fork-silence-editor input.form-control",
    ) as HTMLInputElement;
    fireEvent.change(authorInput, { target: { value: "draft-author" } });

    act(() => {
      silenceFormStore.data.setSilenceID("silence-external-selected");
      silenceFormStore.data.setOperation("edit");
      silenceFormStore.data.setAuthor("selected-author");
    });

    await waitFor(() => {
      expect(
        container.querySelector(".fork-silence-editor"),
      ).toBeInTheDocument();
      expect(container).not.toHaveTextContent("draft-author");
      expect(getByDisplayValue("selected-author")).toBeInTheDocument();
    });
  });
});
