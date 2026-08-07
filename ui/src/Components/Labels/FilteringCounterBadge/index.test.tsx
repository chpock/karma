import { render, screen, fireEvent } from "@testing-library/react";

import { AlertStore, NewUnappliedFilter } from "Stores/AlertStore";
import { QueryOperators } from "Common/Query";
import FilteringCounterBadge from ".";

let alertStore: AlertStore;

beforeEach(() => {
  alertStore = new AlertStore([]);
});

const validateClassName = (
  value: string,
  className: string,
  themed: boolean,
) => {
  render(
    <FilteringCounterBadge
      alertStore={alertStore}
      name="@state"
      value={value}
      counter={1}
      themed={themed}
    />,
  );
  expect(screen.getByText("1")).toHaveClass(className);
};

const validateStyle = (value: string, themed: boolean) => {
  render(
    <FilteringCounterBadge
      alertStore={alertStore}
      name="@state"
      value={value}
      counter={1}
      themed={themed}
    />,
  );
  expect(screen.getByText("1")).not.toHaveAttribute("style");
};

const validateOnClick = (
  value: string,
  themed: boolean,
  isNegative: boolean,
  isAppend: boolean,
  appendFilters: string[] = [],
  filterValue?: string,
  replaceWith?: string,
  expectedFilterCount?: number,
) => {
  alertStore.filters.setFilterValues([NewUnappliedFilter("foo=bar")]);
  render(
    <FilteringCounterBadge
      alertStore={alertStore}
      name="@state"
      value={value}
      counter={1}
      themed={themed}
      isAppend={isAppend}
      appendFilters={appendFilters}
      filterValue={filterValue}
      replaceWith={replaceWith}
    />,
  );
  fireEvent.click(screen.getByText("1"), { altKey: isNegative ? true : false });
  const expectedLength =
    expectedFilterCount ??
    (isAppend ? 2 + appendFilters.length : 1 + appendFilters.length);
  expect(alertStore.filters.values).toHaveLength(expectedLength);
  if (isAppend) {
    expect(alertStore.filters.values).toContainEqual(
      NewUnappliedFilter("foo=bar"),
    );
  }
  expect(alertStore.filters.values).toContainEqual(
    NewUnappliedFilter(
      isNegative
        ? `@state${QueryOperators.NotEqual}${filterValue || value}`
        : replaceWith || `@state${QueryOperators.Equal}${filterValue || value}`,
    ),
  );
  for (const raw of appendFilters) {
    expect(alertStore.filters.values).toContainEqual(NewUnappliedFilter(raw));
  }
};

describe("<FilteringCounterBadge />", () => {
  it("themed @state=unprocessed counter badge should have className 'bg-secondary'", () => {
    validateClassName("unprocessed", "bg-secondary", true);
  });
  it("themed @state=active counter badge should have className 'bg-secondary'", () => {
    validateClassName("active", "bg-danger", true);
  });
  it("themed @state=suppressed counter badge should have className 'bg-secondary'", () => {
    validateClassName("suppressed", "bg-success", true);
  });
  it("themed @state=suppressed-past counter badge should have className 'bg-suppressed-past'", () => {
    validateClassName("suppressed-past", "bg-suppressed-past", true);
  });
  it("unthemed @state=suppressed counter badge should have className 'bg-light'", () => {
    validateClassName("suppressed", "bg-light", false);
  });

  it("@state=unprocessed counter badge should have empty style", () => {
    validateStyle("unprocessed", true);
  });
  it("@state=active counter badge should have empty style", () => {
    validateStyle("active", true);
  });
  it("@state=suppressed counter badge should have empty style", () => {
    validateStyle("suppressed", true);
  });

  it("counter badge should have correct children based on the counter prop value", () => {
    render(
      <FilteringCounterBadge
        alertStore={alertStore}
        name="@state"
        value="active"
        counter={123}
        themed={true}
      />,
    );
    expect(screen.getByText("123")).toBeInTheDocument();
  });

  for (const state of ["unprocessed", "active", "suppressed", "suppressed-past"]) {
    it(`click on @state=${state} counter badge should add a new filter`, () => {
      validateOnClick(state, true, false, true);
    });
    it(`alt+click method on @state=${state} counter badge should add a new negative filter`, () => {
      validateOnClick(state, true, true, true);
    });

    it(`click on @state=${state} counter badge when isAppend=false should replace filters`, () => {
      validateOnClick(state, true, false, false);
    });
    it(`alt+click method on @state=${state} counter badge when isAppend=false should replace filters with a negative one`, () => {
      validateOnClick(state, true, true, false);
    });
  }

  it("suppressed-past click uses @state=suppressed and appends @is_live=false", () => {
    validateOnClick(
      "suppressed-past",
      true,
      false,
      true,
      ["@is_live=false"],
      "suppressed",
      undefined,
      3,
    );
  });

  it("suppressed-past alt+click uses @state!=suppressed without appending @is_live=false", () => {
    alertStore.filters.setFilterValues([NewUnappliedFilter("foo=bar")]);
    render(
      <FilteringCounterBadge
        alertStore={alertStore}
        name="@state"
        value="suppressed-past"
        filterValue="suppressed"
        counter={1}
        themed={true}
        appendFilters={["@is_live=false"]}
      />,
    );

    fireEvent.click(screen.getByText("1"), { altKey: true });
    expect(alertStore.filters.values).toContainEqual(
      NewUnappliedFilter("@state!=suppressed"),
    );
    expect(alertStore.filters.values).not.toContainEqual(
      NewUnappliedFilter("@is_live=false"),
    );
  });
});
