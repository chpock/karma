import { test, expect } from "@playwright/test";

const stories: { name: string; waitFor: string }[] = [
  { name: "Grid", waitFor: "#root > div" },
  { name: "GridColorTitleBar", waitFor: "#root > div" },
  {
    name: "RetainedSuppressionEvidence",
    waitFor: ".fork-suppression-evidence",
  },
  { name: "NavBar", waitFor: "#root > div" },
  { name: "NavBarPaused", waitFor: "#root > div" },
  { name: "FatalError", waitFor: "#root > div" },
  { name: "UpgradeNeeded", waitFor: "#root > div" },
  { name: "ReloadNeeded", waitFor: "#root > div" },
  { name: "EmptyGrid", waitFor: "#root > div" },
  { name: "NoUpstream", waitFor: "#root > div" },
  { name: "InternalError", waitFor: "#root > div" },
  { name: "MainModal", waitFor: ".modal-content" },
  { name: "OverviewModal", waitFor: ".modal-content" },
  { name: "ManagedSilence", waitFor: ".components-managed-silence" },
  { name: "SilenceModalEditorReadOnly", waitFor: ".modal-content" },
  { name: "SilenceModalEditor", waitFor: ".modal-content" },
  { name: "Toast", waitFor: "#root > div" },
];

for (const story of stories) {
  test(story.name, async ({ page }) => {
    await page.clock.install({ time: new Date("2018-08-14T17:00:00Z") });
    await page.goto(`/src/e2e/stories.html#${story.name}`);
    await page.addStyleTag({
      content: "* { caret-color: transparent !important; }",
    });
    if (story.name === "NavBar" || story.name === "NavBarPaused") {
      await page.addStyleTag({
        content:
          ".components-navbar-button { color: transparent !important; }",
      });
    }
    await page.locator(story.waitFor).first().waitFor({ timeout: 10000 });
    if (story.name === "SilenceModalEditor") {
      const editors = page.locator(".fork-silence-editor");
      const count = await editors.count();
      for (let i = 0; i < count; i++) {
        const editor = editors.nth(i);
        const startsSelect = editor.locator("select").nth(1);
        await startsSelect.selectOption("date");

        const startDateInput = editor
          .locator(".fork-silence-editor__timing-row input[type='date']")
          .nth(0);
        const startTimeInput = editor
          .locator(".fork-silence-editor__timing-row input[type='time']")
          .nth(0);

        await startDateInput.fill("2018-08-20");
        await startTimeInput.fill("17:00");
      }
    }
    await page.evaluate(() => {
      if (document.activeElement instanceof HTMLElement) {
        document.activeElement.blur();
      }
    });
    await expect(page).toHaveScreenshot(`${story.name}.png`, {
      fullPage: true,
    });
  });
}

test("SilenceModalEditorPreview", async ({ page }) => {
  await page.clock.install({ time: new Date("2018-08-14T17:00:00Z") });
  await page.goto("/src/e2e/stories.html#SilenceModalEditor");
  await page.addStyleTag({
    content: "* { caret-color: transparent !important; }",
  });

  const editors = page.locator(".fork-silence-editor");
  const count = await editors.count();
  for (let i = 0; i < count; i++) {
    const editor = editors.nth(i);
    await editor.locator(".fork-silence-editor__matcher-expression").click();
    const matcher = editor.getByRole("combobox", {
      name: "Matcher 1 expression, editing",
    });
    await matcher.fill('service="api"');
    await matcher.press("Enter");
  }
  await page.clock.runFor(500);

  const previews = page.locator(".fork-silence-editor__preview");
  await previews
    .first()
    .locator(".fork-silence-editor__preview-item")
    .first()
    .waitFor();
  await expect(previews).toHaveScreenshot("SilenceModalEditorPreview.png");
});

test("SilenceModalEditorLayoutSpacing", async ({ page }) => {
  await page.goto("/src/e2e/stories.html#SilenceModalEditor");

  const editor = page.locator(".fork-silence-editor").first();
  await editor.locator("select").nth(1).selectOption("date");
  await editor.locator("select").nth(2).selectOption("absolute");

  const measurements = await editor.evaluate((node) => {
    const fields = node.querySelectorAll<HTMLElement>(
      ".fork-silence-editor__field",
    );
    const starts = fields[1];
    const ends = fields[2];
    const preview = node.querySelector<HTMLElement>(
      ".fork-silence-editor__preview",
    );
    const grid = node.querySelector<HTMLElement>(".fork-silence-editor__grid");
    if (!starts || !ends || !preview || !grid) {
      throw new Error("expected silence editor layout elements");
    }
    const timingGap = (field: HTMLElement) => {
      const select = field.querySelector<HTMLElement>("select");
      const row = field.querySelector<HTMLElement>(
        ".fork-silence-editor__timing-row",
      );
      if (!select || !row) {
        throw new Error("expected timing controls");
      }
      return row.getBoundingClientRect().top - select.getBoundingClientRect().bottom;
    };
    return {
      startsDetailGap: timingGap(starts),
      endsDetailGap: timingGap(ends),
      previewGap:
        preview.getBoundingClientRect().top - grid.getBoundingClientRect().bottom,
    };
  });

  expect(measurements.startsDetailGap).toBeCloseTo(
    measurements.endsDetailGap,
    0,
  );
  expect(measurements.previewGap).toBeGreaterThanOrEqual(11);
});

test("SilenceModalEditorFocusedTiming", async ({ page }) => {
  await page.goto("/src/e2e/stories.html#SilenceModalEditor");

  const editors = page.locator(".fork-silence-editor");
  const count = await editors.count();
  for (let i = 0; i < count; i++) {
    const timingField = editors
      .nth(i)
      .locator(".fork-silence-editor__field")
      .nth(1);
    const select = timingField.locator("select");
    await select.focus();
    await expect(select).toBeFocused();

    const focusStyle = await select.evaluate((element) => {
      const style = getComputedStyle(element);
      const expected = document.createElement("span");
      expected.style.color =
        "var(--karma-silence-editor-focus-border)";
      element.parentElement?.append(expected);
      const expectedColor = getComputedStyle(expected).color;
      expected.remove();
      return {
        focused: element.matches(":focus"),
        borderWidth: style.borderTopWidth,
        boxShadow: style.boxShadow,
        outlineColor: style.outlineColor,
        expectedColor,
      };
    });
    expect(focusStyle.focused).toBe(true);
    expect(focusStyle.borderWidth).toBe("1px");
    expect(focusStyle.outlineColor).toBe(focusStyle.expectedColor);
    // Bootswatch retains its static inset depth. Focus must never add an outer ring.
    expect(focusStyle.boxShadow).toMatch(/inset$/);

    const matcherInput = editors
      .nth(i)
      .locator(".fork-silence-editor__matcher-expression")
      .first();
    const matcherRestingBoxShadow = await matcherInput.evaluate((element) => {
      const control = element.closest<HTMLElement>(
        ".fork-silence-editor__matcher-control",
      );
      if (!control) {
        throw new Error("expected matcher control");
      }
      return getComputedStyle(control).boxShadow;
    });
    await matcherInput.click();
    const matcherStyle = await matcherInput.evaluate((element) => {
      const control = element.closest<HTMLElement>(
        ".fork-silence-editor__matcher-control",
      );
      if (!control) {
        throw new Error("expected matcher control");
      }
      const style = getComputedStyle(control);
      return {
        boxShadow: style.boxShadow,
      };
    });
    expect(matcherStyle.boxShadow).toBe(matcherRestingBoxShadow);
  }
});

test("SilenceModalEditorMatcherEditing", async ({ page }) => {
  await page.clock.install({ time: new Date("2018-08-14T17:00:00Z") });
  await page.goto("/src/e2e/stories.html#SilenceModalEditor");
  await page.addStyleTag({
    content: "* { caret-color: transparent !important; }",
  });

  const matcherControls = page.locator(
    ".fork-silence-editor__matcher-control",
  );
  const count = await matcherControls.count();
  for (let i = 0; i < count; i++) {
    await matcherControls
      .nth(i)
      .locator(".fork-silence-editor__matcher-expression")
      .click();
  }

  for (let i = 0; i < count; i++) {
    await expect(matcherControls.nth(i)).toHaveScreenshot(
      `SilenceModalEditorMatcherEditing-${i}.png`,
    );
  }
});

test("SilenceModalEditorMatcherSyntax", async ({ page }) => {
  await page.clock.install({ time: new Date("2018-08-14T17:00:00Z") });
  await page.goto("/src/e2e/stories.html#SilenceModalEditor");
  await page.addStyleTag({
    content: "* { caret-color: transparent !important; }",
  });

  const editors = page.locator(".fork-silence-editor");
  const count = await editors.count();
  for (let i = 0; i < count; i++) {
    const input = editors
      .nth(i)
      .locator(".fork-silence-editor__matcher-expression");
    await input.click();
    await input.fill('service=~"api|payments"');
    await input.press("Enter");
  }

  const controls = page.locator(".fork-silence-editor__matcher-control");
  for (let i = 0; i < count; i++) {
    await expect(controls.nth(i)).toHaveScreenshot(
      `SilenceModalEditorMatcherSyntax-${i}.png`,
    );
  }
});

test("SilenceModalEditorMatcherPlaceholder", async ({ page }) => {
  await page.clock.install({ time: new Date("2018-08-14T17:00:00Z") });
  await page.goto("/src/e2e/stories.html#SilenceModalEditor");
  await page.addStyleTag({
    content: "* { caret-color: transparent !important; }",
  });

  const editors = page.locator(".fork-silence-editor");
  const count = await editors.count();
  for (let i = 0; i < count; i++) {
    const editor = editors.nth(i);
    await editor.getByRole("button", { name: "+ Add matcher" }).click();
    const placeholder = editor
      .locator(".fork-silence-editor__matcher-expression")
      .nth(1);
    await expect(placeholder).toHaveScreenshot(
      `SilenceModalEditorMatcherPlaceholder-${i}.png`,
    );
    await placeholder.click();
    await expect(placeholder).toHaveScreenshot(
      `SilenceModalEditorMatcherPlaceholderFocused-${i}.png`,
    );
  }
});

test("SilenceModalEditorMatcherInvalidFocused", async ({ page }) => {
  await page.clock.install({ time: new Date("2018-08-14T17:00:00Z") });
  await page.goto("/src/e2e/stories.html#SilenceModalEditor");
  await page.addStyleTag({
    content: "* { caret-color: transparent !important; }",
  });

  const editors = page.locator(".fork-silence-editor");
  const count = await editors.count();
  for (let i = 0; i < count; i++) {
    const editor = editors.nth(i);
    await editor.getByRole("button", { name: "+ Add matcher" }).click();
    const matcher = editor
      .locator(".fork-silence-editor__matcher-expression")
      .nth(1);
    await matcher.click();
    await expect(
      editor.locator(".fork-silence-editor__matcher-control.invalid").nth(0),
    ).toHaveCSS("border-color", "rgb(231, 76, 60)");
    await expect(
      editor.locator(".fork-silence-editor__matcher-control.invalid").nth(0),
    ).toHaveScreenshot(`SilenceModalEditorMatcherInvalidFocused-${i}.png`);
  }
});

test("SilenceModalEditorAutocomplete", async ({ page }) => {
  await page.clock.install({ time: new Date("2018-08-14T17:00:00Z") });
  await page.goto("/src/e2e/stories.html#SilenceModalEditor");
  await page.addStyleTag({
    content:
      "* { caret-color: transparent !important; } .fork-silence-editor__suggestions { scrollbar-width: none; } .fork-silence-editor__suggestions::-webkit-scrollbar { display: none; } .fork-silence-editor__suggestion { pointer-events: none; }",
  });

  const editors = page.locator(".fork-silence-editor");
  await editors.first().getByRole("button", { name: "+ Add matcher" }).click();
  await page.clock.runFor(250);

  const firstEditor = editors.first();
  const firstMenu = firstEditor.locator(".fork-silence-editor__suggestions");
  await firstMenu.waitFor();
  const selectedItem = firstMenu.locator('[aria-selected="true"]');
  await expect(selectedItem).toHaveClass(
    /fork-silence-editor__suggestion active/,
  );

  await expect
    .poll(() => firstMenu.locator('[role="option"]').count())
    .toBeGreaterThan(5);
  const [menuBox, modalBox] = await Promise.all([
    firstMenu.boundingBox(),
    page.locator(".modal-content").first().boundingBox(),
  ]);
  expect(menuBox).not.toBeNull();
  expect(modalBox).not.toBeNull();
  expect(menuBox!.width).toBeGreaterThan(160);
  expect(menuBox!.x).toBeGreaterThanOrEqual(modalBox!.x);
  expect(menuBox!.x + menuBox!.width).toBeLessThanOrEqual(
    modalBox!.x + modalBox!.width,
  );
  await expect
    .poll(() =>
      firstMenu.evaluate((menu) => menu.scrollWidth - menu.clientWidth),
    )
    .toBe(0);
  const firstMatcher = firstEditor
    .locator(".fork-silence-editor__matcher-expression")
    .nth(1);
  const initialMetrics = await firstMenu.evaluate((menu) => ({
    clientHeight: menu.clientHeight,
    scrollHeight: menu.scrollHeight,
  }));
  expect(initialMetrics.scrollHeight).toBeGreaterThan(
    initialMetrics.clientHeight,
  );
  for (let i = 0; i < 8; i++) {
    await firstMatcher.press("ArrowDown");
  }
  await expect(firstMenu.locator('[aria-selected="true"]')).toContainText(
    "prometheus",
  );
  await expect
    .poll(() => firstMenu.evaluate((menu) => menu.scrollTop))
    .toBeGreaterThan(0);
  await expect(firstMenu.locator('[aria-selected="true"]')).toBeInViewport();
  const beforeScroll = await firstMenu.evaluate((menu) => menu.scrollTop);
  await firstMenu.evaluate((menu) => {
    menu.scrollTop = menu.scrollHeight;
  });
  await expect
    .poll(() => firstMenu.evaluate((menu) => menu.scrollTop))
    .toBeGreaterThan(beforeScroll);
  await expect(firstMenu).toBeVisible();
});

test("SilenceModalEditorEndsAtDateTime", async ({ page }) => {
  await page.clock.install({ time: new Date("2018-08-14T17:00:00Z") });
  await page.goto(`/src/e2e/stories.html#SilenceModalEditor`);
  await page.addStyleTag({
    content: "* { caret-color: transparent !important; }",
  });
  await page.locator(".modal-content").first().waitFor({ timeout: 10000 });

  const editors = page.locator(".fork-silence-editor");
  const count = await editors.count();
  for (let i = 0; i < count; i++) {
    const editor = editors.nth(i);
    const endsSelect = editor.locator("select").nth(2);
    await endsSelect.selectOption("absolute");

    const dateInput = editor
      .locator(".fork-silence-editor__timing-row input[type='date']")
      .nth(0);
    const timeInput = editor
      .locator(".fork-silence-editor__timing-row input[type='time']")
      .nth(0);

    await dateInput.fill("2018-08-20");
    await timeInput.fill("18:45");
  }

  await page.evaluate(() => {
    if (document.activeElement instanceof HTMLElement) {
      document.activeElement.blur();
    }
  });

  await expect(page).toHaveScreenshot("SilenceModalEditorEndsAtDateTime.png", {
    fullPage: true,
  });
});
