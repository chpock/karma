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
