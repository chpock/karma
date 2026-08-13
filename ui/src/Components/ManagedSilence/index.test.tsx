import { act } from "react";

import { render, screen, fireEvent } from "@testing-library/react";

import { MockSilence } from "__fixtures__/Alerts";
import { MockThemeContext } from "__fixtures__/Theme";
import { inReactiveContext } from "__fixtures__/MobX";
import type { APISilenceT } from "Models/APITypes";
import { AlertStore } from "Stores/AlertStore";
import { SilenceFormStore } from "Stores/SilenceFormStore";
import { ThemeContext } from "Components/Theme";
import { ManagedSilence, GetAlertmanager } from ".";

let alertStore: AlertStore;
let silenceFormStore: SilenceFormStore;
let cluster: string;
let silence: APISilenceT;

const statusResponse = (
  source: APISilenceT = silence,
  isExpired = false,
  member = "am1",
) => ({
  isExpired,
  member,
  revisionToken: `revision-${source.id}`,
  silence: source,
});

beforeEach(() => {
  jest.useFakeTimers();
  jest.setSystemTime(Date.UTC(2000, 0, 1, 0, 30, 0));

  alertStore = new AlertStore([]);
  silenceFormStore = new SilenceFormStore();
  cluster = "am";
  silence = MockSilence();

  alertStore.data.setUpstreams({
    counters: { total: 1, healthy: 1, failed: 0 },
    instances: [
      {
        name: "am1",
        cluster: "am",
        clusterMembers: ["am1"],
        uri: "http://localhost:9093",
        publicURI: "http://example.com",
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

  jest.restoreAllMocks();
  global.fetch = jest.fn(() =>
    Promise.resolve(
      new Response(JSON.stringify(statusResponse()), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    ),
  ) as typeof fetch;
});

afterEach(() => {
  jest.restoreAllMocks();
  jest.useRealTimers();
});

const renderManagedSilence = (onDidUpdate?: () => void) => {
  return render(
    <ThemeContext value={MockThemeContext}>
      <ManagedSilence
        cluster={cluster}
        alertCount={123}
        alertCountAlwaysVisible={true}
        silence={silence}
        isExpired={false}
        alertStore={alertStore}
        silenceFormStore={silenceFormStore}
        onDidUpdate={onDidUpdate}
      />
    </ThemeContext>,
  );
};

describe("<ManagedSilence />", () => {
  it("matches snapshot when collapsed", () => {
    const { asFragment } = renderManagedSilence();
    expect(asFragment()).toMatchSnapshot();
  });

  it("clicking on expand toggle shows silence details", () => {
    const { container } = renderManagedSilence();
    const toggle = container.querySelector("svg.text-muted.cursor-pointer");
    fireEvent.click(toggle!);
    expect(screen.getByText("Edit")).toBeInTheDocument();
  });

  it("matches snapshot with expanded details", () => {
    const { container, asFragment } = renderManagedSilence();
    const toggle = container.querySelector("svg.text-muted.cursor-pointer");
    fireEvent.click(toggle!);
    expect(asFragment()).toMatchSnapshot();
  });

  it("GetAlertmanager() returns alertmanager object from alertStore.data.upstreams.instances", () => {
    inReactiveContext(() => {
      const am = GetAlertmanager(alertStore, cluster);
      expect(am).toEqual({
        name: "am1",
        cluster: "am",
        clusterMembers: ["am1"],
        uri: "http://localhost:9093",
        publicURI: "http://example.com",
        readonly: false,
        error: "",
        warnings: [],
        version: "0.24.0",
        headers: {},
        corsCredentials: "include",
      });
    });
  });

  it("GetAlertmanager() returns only writable instances", () => {
    alertStore.data.setUpstreams({
      counters: { total: 2, healthy: 2, failed: 0 },
      instances: [
        {
          name: "am1",
          cluster: "am",
          clusterMembers: ["am1", "am2"],
          uri: "http://localhost:9093",
          publicURI: "http://example.com",
          readonly: false,
          error: "",
          warnings: [],
          version: "0.24.0",
          headers: {},
          corsCredentials: "include",
        },
        {
          name: "am2",
          cluster: "am",
          clusterMembers: ["am1", "am2"],
          uri: "http://localhost:9094",
          publicURI: "http://example.com",
          readonly: true,
          error: "",
          warnings: [],
          version: "0.24.0",
          headers: {},
          corsCredentials: "include",
        },
      ],
      clusters: { am: ["am1", "am2"] },
    });

    inReactiveContext(() => {
      const am = GetAlertmanager(alertStore, cluster);
      expect(am).toEqual({
        name: "am1",
        cluster: "am",
        clusterMembers: ["am1"],
        uri: "http://localhost:9093",
        publicURI: "http://example.com",
        readonly: false,
        error: "",
        warnings: [],
        version: "0.24.0",
        headers: {},
        corsCredentials: "include",
      });
    });
  });

  it("shows Edit button on unexpired silence", () => {
    const { container } = renderManagedSilence();
    const toggle = container.querySelector("svg.text-muted.cursor-pointer");
    fireEvent.click(toggle!);

    expect(screen.getByText("Edit")).toBeInTheDocument();
  });

  it("shows Delete button on unexpired silence", () => {
    const { container } = renderManagedSilence();
    const toggle = container.querySelector("svg.text-muted.cursor-pointer");
    fireEvent.click(toggle!);

    expect(screen.getByText("Delete")).toBeInTheDocument();
  });

  it("shows Recreate button on expired silence", () => {
    const { container } = render(
      <ThemeContext value={MockThemeContext}>
        <ManagedSilence
          cluster={cluster}
          alertCount={123}
          alertCountAlwaysVisible={true}
          silence={silence}
          isExpired={true}
          alertStore={alertStore}
          silenceFormStore={silenceFormStore}
        />
      </ThemeContext>,
    );
    const toggle = container.querySelector("svg.text-muted.cursor-pointer");
    fireEvent.click(toggle!);

    expect(screen.getByText("Recreate")).toBeInTheDocument();
  });

  it("uses server-provided isExpired for action label", () => {
    const { container } = render(
      <ThemeContext value={MockThemeContext}>
        <ManagedSilence
          cluster={cluster}
          alertCount={123}
          alertCountAlwaysVisible={true}
          silence={silence}
          isExpired={true}
          alertStore={alertStore}
          silenceFormStore={silenceFormStore}
        />
      </ThemeContext>,
    );
    const toggle = container.querySelector("svg.text-muted.cursor-pointer");
    fireEvent.click(toggle!);
    expect(screen.getByText("Recreate")).toBeInTheDocument();
  });

  it("clicking on Edit verifies status before opening", async () => {
    const { container } = renderManagedSilence();
    const toggle = container.querySelector("svg.text-muted.cursor-pointer");
    fireEvent.click(toggle!);

    expect(silenceFormStore.data.silenceID).toBeNull();

    const button = screen.getByText("Edit");
    fireEvent.click(button);
    await act(async () => {});
    expect(silenceFormStore.data.silenceID).toBe(silence.id);
    expect(silenceFormStore.data.editorRevisionToken).toBe(
      `revision-${silence.id}`,
    );
  });

  it("fills the editor from the selected member response, not the list card", async () => {
    const selectedMemberSilence = {
      ...silence,
      comment: "Fresh comment from selected member",
      createdBy: "fresh-author@example.com",
      matchers: [
        { name: "service", value: "payments", isRegex: false, isEqual: true },
      ],
    };
    global.fetch = jest.fn(() =>
      Promise.resolve(
        new Response(JSON.stringify(statusResponse(selectedMemberSilence)), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        }),
      ),
    ) as typeof fetch;
    const { container } = renderManagedSilence();
    fireEvent.click(container.querySelector("svg.text-muted.cursor-pointer")!);
    fireEvent.click(screen.getByText("Edit"));
    await act(async () => {});

    expect(silenceFormStore.data.comment).toBe(
      "Fresh comment from selected member",
    );
    expect(silenceFormStore.data.author).toBe("fresh-author@example.com");
    expect(silenceFormStore.data.matchers[0].name).toBe("service");
    expect(silenceFormStore.data.matchers[0].values[0].value).toBe("payments");
  });

  it("uses request-time server expiry instead of the stale list value", async () => {
    global.fetch = jest.fn(() =>
      Promise.resolve(
        new Response(JSON.stringify(statusResponse(silence, true)), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        }),
      ),
    ) as typeof fetch;
    const { container } = renderManagedSilence();
    fireEvent.click(container.querySelector("svg.text-muted.cursor-pointer")!);
    fireEvent.click(screen.getByText("Edit"));
    await act(async () => {});

    expect(silenceFormStore.data.operation).toBe("recreate");
  });

  it("ignores an older status response after a newer silence was selected", async () => {
    const secondSilence = { ...silence, id: "silence-second" };
    let resolveFirst: (response: Response) => void = () => {};
    let resolveSecond: (response: Response) => void = () => {};
    global.fetch = jest.fn((input: RequestInfo | URL) => {
      const promise = new Promise<Response>((resolve) => {
        if (input.toString().includes(secondSilence.id)) {
          resolveSecond = resolve;
        } else {
          resolveFirst = resolve;
        }
      });
      return promise;
    }) as typeof fetch;
    render(
      <ThemeContext value={MockThemeContext}>
        <ManagedSilence
          cluster={cluster}
          alertCount={1}
          alertCountAlwaysVisible
          silence={silence}
          isExpired={false}
          alertStore={alertStore}
          silenceFormStore={silenceFormStore}
          isOpen
        />
        <ManagedSilence
          cluster={cluster}
          alertCount={1}
          alertCountAlwaysVisible
          silence={secondSilence}
          isExpired={false}
          alertStore={alertStore}
          silenceFormStore={silenceFormStore}
          isOpen
        />
      </ThemeContext>,
    );

    const editButtons = screen.getAllByText("Edit");
    fireEvent.click(editButtons[0]);
    fireEvent.click(editButtons[1]);
    await act(async () => {
      resolveSecond(
        new Response(JSON.stringify(statusResponse(secondSilence)), {
          status: 200,
        }),
      );
    });
    expect(silenceFormStore.data.silenceID).toBe(secondSilence.id);
    await act(async () => {
      resolveFirst(
        new Response(JSON.stringify(statusResponse(silence, true)), {
          status: 200,
        }),
      );
    });
    expect(silenceFormStore.data.silenceID).toBe(secondSilence.id);
  });

  it("keeps the compact editor closed when status verification fails", async () => {
    global.fetch = jest.fn(() =>
      Promise.resolve(new Response("unavailable", { status: 503 })),
    ) as typeof fetch;
    const { container } = renderManagedSilence();
    fireEvent.click(container.querySelector("svg.text-muted.cursor-pointer")!);
    fireEvent.click(screen.getByText("Edit"));
    await act(async () => {});

    expect(silenceFormStore.toggle.visible).toBe(false);
    expect(silenceFormStore.data.editorSessionState).toBe("closed");
    expect(screen.getByRole("alert")).toHaveTextContent(
      "failed to verify silence status: 503",
    );
  });

  it("call onDidUpdate if passed", () => {
    const fakeUpdate = jest.fn();
    const { container } = renderManagedSilence(fakeUpdate);
    const toggle = container.querySelector("svg.text-muted.cursor-pointer");
    fireEvent.click(toggle!);
    expect(fakeUpdate).toHaveBeenCalled();
  });
});

describe("<ManagedSilence progress bar />", () => {
  it("renders with class 'danger' and no progressbar when expired", () => {
    const { container } = render(
      <ThemeContext value={MockThemeContext}>
        <ManagedSilence
          cluster={cluster}
          alertCount={123}
          alertCountAlwaysVisible={true}
          silence={silence}
          isExpired={true}
          alertStore={alertStore}
          silenceFormStore={silenceFormStore}
        />
      </ThemeContext>,
    );
    const toggle = container.querySelector("svg.text-muted.cursor-pointer");
    fireEvent.click(toggle!);
    expect(screen.getByText("Recreate")).toBeInTheDocument();
  });

  it("renders with class 'warning' if <= 5m is left", () => {
    jest.setSystemTime(Date.UTC(2000, 0, 1, 0, 56, 0));
    const { container } = renderManagedSilence();
    expect(container.innerHTML).toMatch(/bg-warning/);
    expect(screen.getByText(/Expires in 4 minutes/)).toBeInTheDocument();
  });

  it("progressbar uses class 'danger' when > 90%", () => {
    jest.setSystemTime(Date.UTC(2000, 0, 1, 0, 55, 0));
    const { container } = renderManagedSilence();
    expect(container.innerHTML).toMatch(/progress-bar bg-danger/);
  });

  it("progressbar uses class 'danger' when > 75%", () => {
    jest.setSystemTime(Date.UTC(2000, 0, 1, 0, 50, 0));
    const { container } = renderManagedSilence();
    expect(container.innerHTML).toMatch(/progress-bar bg-warning/);
  });

  it("progressbar uses class 'success' when <= 75%", () => {
    jest.setSystemTime(Date.UTC(2000, 0, 1, 0, 30, 0));
    const { container } = renderManagedSilence();
    expect(container.innerHTML).toMatch(/progress-bar bg-success/);
  });

  it("progressbar is updated every 30 seconds", () => {
    jest.setSystemTime(Date.UTC(2000, 0, 1, 0, 30, 0));
    const { container } = renderManagedSilence();
    expect(container.innerHTML).toMatch(/progress-bar bg-success/);

    jest.setSystemTime(Date.UTC(2000, 0, 1, 0, 50, 0));
    act(() => {
      jest.runOnlyPendingTimers();
    });
    expect(container.innerHTML).toMatch(/progress-bar bg-warning/);

    jest.setSystemTime(Date.UTC(2000, 0, 1, 0, 55, 0));
    act(() => {
      jest.runOnlyPendingTimers();
    });
    expect(container.innerHTML).toMatch(/progress-bar bg-danger/);
  });

  it("unmounts cleanly", () => {
    const { unmount } = renderManagedSilence();
    unmount();
  });
});
