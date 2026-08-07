import { render } from "@testing-library/react";

import type { APIGridT } from "Models/APITypes";
import { AlertStore } from "Stores/AlertStore";
import { Settings } from "Stores/Settings";
import { Swimlane } from "./Swimlane";

const baseGrid = (): APIGridT => ({
  labelName: "",
  labelValue: "",
  alertGroups: [],
  totalGroups: 0,
  stateCount: {
    active: 0,
    suppressed: 0,
    unprocessed: 0,
    inactive: 0,
  },
});

describe("<Swimlane /> suppressed badges", () => {
  it("renders current suppressed badge from suppressedBreakdown", () => {
    const grid = baseGrid();
    grid.stateCount.suppressed = 5;
    grid.suppressedBreakdown = { current: 5, past: 0 };

    const { container } = render(
      <Swimlane
        alertStore={new AlertStore([])}
        settingsStore={new Settings(null)}
        grid={grid}
        isExpanded={true}
        onToggle={jest.fn()}
        paddingTop={0}
      />,
    );

    const suppressedBadge = container.querySelector(".badge.bg-success");
    expect(suppressedBadge).toBeInTheDocument();
    expect(suppressedBadge?.textContent).toBe("5");
  });

  it("renders only past badge when current=0 and past>0", () => {
    const grid = baseGrid();
    grid.stateCount.suppressed = 3;
    grid.suppressedBreakdown = { current: 0, past: 3 };

    const { container } = render(
      <Swimlane
        alertStore={new AlertStore([])}
        settingsStore={new Settings(null)}
        grid={grid}
        isExpanded={true}
        onToggle={jest.fn()}
        paddingTop={0}
      />,
    );

    expect(container.querySelector(".badge.bg-success")).toBeNull();
    const pastBadge = container.querySelector(".badge.bg-suppressed-past");
    expect(pastBadge).toBeInTheDocument();
    expect(pastBadge?.textContent).toBe("3");
  });

  it("renders split current/past badges from suppressedBreakdown", () => {
    const grid = baseGrid();
    grid.stateCount.suppressed = 2;
    grid.suppressedBreakdown = { current: 1, past: 1 };

    const { container } = render(
      <Swimlane
        alertStore={new AlertStore([])}
        settingsStore={new Settings(null)}
        grid={grid}
        isExpanded={true}
        onToggle={jest.fn()}
        paddingTop={0}
      />,
    );

    const currentBadge = container.querySelector(".badge.bg-success");
    const pastBadge = container.querySelector(".badge.bg-suppressed-past");
    expect(currentBadge).toBeInTheDocument();
    expect(currentBadge?.textContent).toBe("1");
    expect(pastBadge).toBeInTheDocument();
    expect(pastBadge?.textContent).toBe("1");
  });
});
