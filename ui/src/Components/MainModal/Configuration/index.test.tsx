import { render } from "@testing-library/react";

import fetchMock from "@fetch-mock/jest";

import { MockThemeContext } from "__fixtures__/Theme";
import { AlertStore } from "Stores/AlertStore";
import { Settings } from "Stores/Settings";
import { ThemeContext } from "Components/Theme";
import { Configuration } from ".";

beforeEach(() => {
  fetchMock.mockReset();
  fetchMock.route("*", {
    status: 200,
    body: JSON.stringify([]),
  });
});

afterEach(() => {
  jest.restoreAllMocks();
  fetchMock.mockReset();
});

describe("<Configuration />", () => {
  it("matches snapshot", () => {
    const alertStore = new AlertStore([]);
    const settingsStore = new Settings(null);
    const { asFragment } = render(
      <ThemeContext value={MockThemeContext}>
        <Configuration
          alertStore={alertStore}
          settingsStore={settingsStore}
          defaultIsOpen={true}
        />
      </ThemeContext>,
    );
    expect(asFragment()).toMatchSnapshot();
  });
});
