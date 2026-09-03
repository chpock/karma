import { test, expect, type Page } from "@playwright/test";

const stories: { name: string; title?: string; waitFor: string }[] = [
  { name: "Grid", waitFor: "#root > div" },
  { name: "GridColorTitleBar", waitFor: "#root > div" },
  {
    name: "RetainedSuppressionEvidence",
    waitFor: ".components-managed-silence",
  },
  { name: "NavBar", waitFor: "#root > div" },
  { name: "NavBarPaused", waitFor: "#root > div" },
  {
    name: "ForkLifecycleWarning",
    title: "Fork lifecycle warning",
    waitFor: '[role="alert"]',
  },
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
  { name: "SilenceModalEditorValidation", waitFor: ".modal-content" },
  { name: "Toast", waitFor: "#root > div" },
];

for (const story of stories) {
  test(story.title || story.name, async ({ page }) => {
    if (story.name === "ForkLifecycleWarning") {
      test.skip(
        ["webkit", "iphone"].includes(test.info().project.name),
        "Problem 4 warning visual baselines are scoped to installed Chromium, Firefox, and Android projects",
      );
    }
    await page.clock.install({ time: new Date("2018-08-14T17:00:00Z") });
    await page.goto(`/src/e2e/stories.html#${story.name}`);
    await page.addStyleTag({
      content: "* { caret-color: transparent !important; }",
    });
    if (story.name === "NavBar" || story.name === "NavBarPaused") {
      await page.addStyleTag({
        content: ".components-navbar-button { color: transparent !important; }",
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
    await expect(page.locator("#root")).toHaveScreenshot(`${story.name}.png`);
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
  const firstPreview = previews.first();
  const expectedIdentityDirection = ["iphone", "android"].includes(
    test.info().project.name,
  )
    ? "row"
    : "column";
  await expect(
    firstPreview.locator(".fork-silence-editor__preview-identity").first(),
  ).toHaveCSS("flex-direction", expectedIdentityDirection);
  await expect(
    firstPreview.locator(".fork-silence-editor__preview-alertname"),
  ).toHaveCount(3);
  await expect(
    firstPreview.locator(".fork-silence-editor__preview-labels"),
  ).toHaveCount(3);
  const previewColors = await firstPreview.evaluate((preview) => {
    const item = preview.querySelector<HTMLElement>(
      ".fork-silence-editor__preview-item",
    );
    const matched = preview.querySelector<HTMLElement>(
      ".fork-silence-editor__preview-label--matched",
    );
    const ordinary = preview.querySelector<HTMLElement>(
      ".fork-silence-editor__preview-label:not(.fork-silence-editor__preview-label--matched):not(.fork-silence-editor__preview-label--matched-only):not(.fork-silence-editor__preview-label--missing)",
    );
    const matcherLabel = preview
      .closest(".fork-silence-editor")
      ?.querySelector<HTMLElement>(
        ".fork-silence-editor__matcher-highlight .syntax-label",
      );
    const previewLabel = ordinary?.querySelector<HTMLElement>(".syntax-label");
    return {
      card: item && getComputedStyle(item).backgroundColor,
      cardBorder: item && getComputedStyle(item).borderTopColor,
      matched: matched && getComputedStyle(matched).backgroundColor,
      preview: getComputedStyle(preview).backgroundColor,
      ordinary: ordinary && getComputedStyle(ordinary).backgroundColor,
      matcherLabel: matcherLabel && getComputedStyle(matcherLabel).color,
      previewLabel: previewLabel && getComputedStyle(previewLabel).color,
      previewLabelFontSize: ordinary && getComputedStyle(ordinary).fontSize,
      previewLabelFontWeight: ordinary && getComputedStyle(ordinary).fontWeight,
    };
  });
  expect(previewColors.card).toBe("rgba(0, 0, 0, 0)");
  expect(previewColors.cardBorder).not.toBe("rgba(0, 0, 0, 0)");
  expect(previewColors.cardBorder).not.toBe(previewColors.preview);
  expect(previewColors.matched).not.toBe(previewColors.preview);
  expect(previewColors.ordinary).toBe(previewColors.preview);
  expect(previewColors.previewLabel).toBe(previewColors.matcherLabel);
  expect(previewColors.previewLabelFontSize).toBe("11.2px");
  expect(previewColors.previewLabelFontWeight).toBe("400");
  const darkMatchedColors = await previews.nth(1).evaluate((preview) => {
    const matched = preview.querySelector<HTMLElement>(
      ".fork-silence-editor__preview-label--matched",
    );
    if (!matched) {
      throw new Error("expected matched preview label");
    }
    const rgb = (color: string): [number, number, number] => {
      const match = color.match(/\d+/g);
      if (!match || match.length < 3) {
        throw new Error(`expected RGB color, got ${color}`);
      }
      return [Number(match[0]), Number(match[1]), Number(match[2])];
    };
    const luminance = ([red, green, blue]: [number, number, number]) => {
      const linear = [red, green, blue].map((channel) => {
        const normalized = channel / 255;
        return normalized <= 0.04045
          ? normalized / 12.92
          : ((normalized + 0.055) / 1.055) ** 2.4;
      });
      return linear[0] * 0.2126 + linear[1] * 0.7152 + linear[2] * 0.0722;
    };
    const contrast = (foreground: string, background: string) => {
      const [first, second] = [
        luminance(rgb(foreground)),
        luminance(rgb(background)),
      ];
      return (
        (Math.max(first, second) + 0.05) / (Math.min(first, second) + 0.05)
      );
    };
    const styles = getComputedStyle(matched);
    return {
      background: styles.backgroundColor,
      labelContrast: contrast(
        getComputedStyle(matched.querySelector(".syntax-label")!).color,
        styles.backgroundColor,
      ),
      operatorContrast: contrast(
        getComputedStyle(matched.querySelector(".syntax-operator")!).color,
        styles.backgroundColor,
      ),
      valueContrast: contrast(
        getComputedStyle(matched.querySelector(".syntax-value")!).color,
        styles.backgroundColor,
      ),
      labelColor: getComputedStyle(matched.querySelector(".syntax-label")!)
        .color,
      valueColor: getComputedStyle(matched.querySelector(".syntax-value")!)
        .color,
    };
  });
  expect(darkMatchedColors.background).toBe("rgb(169, 189, 204)");
  expect(darkMatchedColors.labelContrast).toBeGreaterThanOrEqual(4.5);
  expect(darkMatchedColors.operatorContrast).toBeGreaterThanOrEqual(4.5);
  expect(darkMatchedColors.valueContrast).toBeGreaterThanOrEqual(4.5);
  expect(darkMatchedColors.labelColor).toBe("rgb(16, 42, 67)");
  expect(darkMatchedColors.valueColor).toBe("rgb(15, 77, 38)");
  if (!["iphone", "android"].includes(test.info().project.name)) {
    const viewport = page.viewportSize();
    if (viewport === null) {
      throw new Error("expected viewport size");
    }
    await expect(page.locator(".modal-dialog").first()).toHaveCSS(
      "max-height",
      `${viewport.height - 24}px`,
    );
    await expect(
      firstPreview.locator(".fork-silence-editor__preview-list"),
    ).toHaveCSS("max-height", "none");
    await expect(page.locator(".modal-body").first()).toHaveCSS(
      "overflow-y",
      "hidden",
    );
    await expect(
      firstPreview.locator(".fork-silence-editor__preview-list"),
    ).toHaveCSS("overflow-y", "auto");
  }
  await expect(previews).toHaveScreenshot("SilenceModalEditorPreview.png");
  if (!["iphone", "android"].includes(test.info().project.name)) {
    const scrollOwnership = await firstPreview.evaluate((preview) => {
      const list = preview.querySelector<HTMLElement>(
        ".fork-silence-editor__preview-list",
      );
      const item = list?.querySelector<HTMLElement>(
        ".fork-silence-editor__preview-item",
      );
      const body = preview.closest<HTMLElement>(".modal-body");
      if (!list || !item || !body) {
        throw new Error("expected preview scroll container");
      }
      for (let index = 0; index < 24; index++) {
        list.append(item.cloneNode(true));
      }
      list.scrollTop = list.scrollHeight;
      return {
        bodyOverflows: body.scrollHeight > body.clientHeight + 1,
        listOverflows: list.scrollHeight > list.clientHeight,
        listScrollTop: list.scrollTop,
      };
    });
    expect(scrollOwnership.bodyOverflows).toBe(false);
    expect(scrollOwnership.listOverflows).toBe(true);
    expect(scrollOwnership.listScrollTop).toBeGreaterThan(0);
  }
  const longLabel = firstPreview
    .locator(".fork-silence-editor__preview-label")
    .first();
  const longLabelLayout = await longLabel.evaluate((label) => {
    const syntaxLabel = label.querySelector<HTMLElement>(".syntax-label");
    const syntaxValue = label.querySelector<HTMLElement>(".syntax-value");
    const labels = label.parentElement;
    if (!syntaxLabel || !syntaxValue || !labels) {
      throw new Error("expected preview label contents");
    }
    syntaxLabel.textContent = "label".repeat(80);
    syntaxValue.textContent = "value".repeat(160);
    const labelRect = label.getBoundingClientRect();
    const labelsRect = labels.getBoundingClientRect();
    const styles = getComputedStyle(label);
    return {
      overflows: label.scrollWidth > label.clientWidth,
      fitsInContainer: labelRect.right <= labelsRect.right + 1,
      maxWidth: styles.maxWidth,
      minWidth: styles.minWidth,
      overflow: styles.overflow,
      textOverflow: styles.textOverflow,
      whiteSpace: styles.whiteSpace,
    };
  });
  expect(longLabelLayout).toEqual({
    overflows: true,
    fitsInContainer: true,
    maxWidth: "100%",
    minWidth: "0px",
    overflow: "hidden",
    textOverflow: "ellipsis",
    whiteSpace: "nowrap",
  });
});

const measureProblem4WarningState = async (
  page: Page,
  state: string,
  snapshotName: string,
) => {
  const evidence = await page.evaluate((currentState) => {
    const parseRGB = (value: string): [number, number, number] => {
      const channels = value.match(/\d+(?:\.\d+)?/g);
      if (!channels || channels.length < 3) {
        throw new Error(`expected RGB color, got ${value}`);
      }
      return [Number(channels[0]), Number(channels[1]), Number(channels[2])];
    };
    const luminance = ([red, green, blue]: [number, number, number]) => {
      const linear = [red, green, blue].map((channel) => {
        const normalized = channel / 255;
        return normalized <= 0.04045
          ? normalized / 12.92
          : ((normalized + 0.055) / 1.055) ** 2.4;
      });
      return linear[0] * 0.2126 + linear[1] * 0.7152 + linear[2] * 0.0722;
    };
    const contrast = (first: string, second: string) => {
      const firstLuminance = luminance(parseRGB(first));
      const secondLuminance = luminance(parseRGB(second));
      return (
        (Math.max(firstLuminance, secondLuminance) + 0.05) /
        (Math.min(firstLuminance, secondLuminance) + 0.05)
      );
    };
    const pageBackground = getComputedStyle(document.body).backgroundColor;
    const cards = Array.from(document.querySelectorAll<HTMLElement>('[role="alert"]')).map((card) => {
      const styles = getComputedStyle(card);
      const text = card.querySelector<HTMLElement>("strong") || card;
      return {
        kind: card.getAttribute("data-issue-kind"),
        background: styles.backgroundColor,
        border: styles.borderTopColor,
        foreground: getComputedStyle(text).color,
        textContrast: contrast(getComputedStyle(text).color, styles.backgroundColor),
        borderContrast: contrast(styles.borderTopColor, pageBackground),
      };
    });
    const controls = Array.from(document.querySelectorAll<HTMLElement>("button, input")).map((control) => {
      const styles = getComputedStyle(control);
      return {
        label: control.textContent?.trim() || (control as HTMLInputElement).value,
        disabled: (control as HTMLButtonElement | HTMLInputElement).disabled,
        color: styles.color,
        background: styles.backgroundColor,
        border: styles.borderTopColor,
        borderStyle: styles.borderTopStyle,
        textContrast: contrast(styles.color, styles.backgroundColor),
        borderContrast: contrast(styles.borderTopColor, pageBackground),
      };
    });
    return { state: currentState, cards, controls };
  }, state);
  for (const card of evidence.cards) {
    expect(card.textContrast, JSON.stringify(card)).toBeGreaterThanOrEqual(4.5);
    expect(card.borderContrast, JSON.stringify(card)).toBeGreaterThanOrEqual(3);
  }
  for (const control of evidence.controls) {
    expect(control.borderStyle, JSON.stringify(control)).not.toBe("none");
    expect(control.textContrast, JSON.stringify(control)).toBeGreaterThanOrEqual(4.5);
    expect(control.borderContrast, JSON.stringify(control)).toBeGreaterThanOrEqual(3);
  }
  await expect(JSON.stringify(evidence, null, 2)).toMatchSnapshot(snapshotName);
  return evidence;
};

const runForkLifecycleWarningInteractiveVisual = async (
  page: Page,
  themeName: "light" | "dark",
) => {
  await page.clock.install({ time: new Date("2018-08-14T17:00:00Z") });
  let theme = page.locator(`.theme-${themeName}`);
  const visualEvidence: Array<{ state: string; target: string | null; controls: unknown[] }> = [];
  const loadState = async (visualState: string) => {
    if (page.url() === "about:blank") await page.goto("/src/e2e/stories.html");
    await page.evaluate(
      ({ state, theme }) => {
        sessionStorage.setItem("forkLifecycleWarningVisualState", state);
        sessionStorage.setItem("forkLifecycleWarningVisualTheme", theme);
      },
      { state: visualState, theme: themeName },
    );
    await page.goto(`/src/e2e/stories.html#ForkLifecycleWarning`);
    await page.reload();
    await page.addStyleTag({ content: "*, *::before, *::after { caret-color: transparent !important; transition: none !important; animation: none !important; }" });
    theme = page.locator(`.theme-${themeName}`);
    await theme.getByRole("alert").last().waitFor();
  };
  await loadState("resting");
  const colors = await theme.getByRole("button").evaluateAll((buttons) =>
    buttons.map((button) => {
      const styles = getComputedStyle(button);
      const parseRGB = (value: string): [number, number, number] => {
        const channels = value.match(/\d+(?:\.\d+)?/g);
        if (!channels || channels.length < 3)
          throw new Error(`expected RGB color, got ${value}`);
        return [Number(channels[0]), Number(channels[1]), Number(channels[2])];
      };
      const luminance = (rgb: [number, number, number]) =>
        rgb.reduce((sum, channel, index) => {
          const normalized = channel / 255;
          const linear =
            normalized <= 0.04045
              ? normalized / 12.92
              : ((normalized + 0.055) / 1.055) ** 2.4;
          return sum + linear * [0.2126, 0.7152, 0.0722][index];
        }, 0);
      const foreground = luminance(parseRGB(styles.color));
      const background = luminance(parseRGB(styles.backgroundColor));
      return {
        color: styles.color,
        background: styles.backgroundColor,
        border: styles.borderTopColor,
        contrast:
          (Math.max(foreground, background) + 0.05) /
          (Math.min(foreground, background) + 0.05),
      };
    }),
  );
  for (const color of colors) {
    expect(color.color).not.toBe(color.background);
    expect(color.border).not.toBe("rgba(0, 0, 0, 0)");
    expect(color.contrast).toBeGreaterThanOrEqual(4.5);
  }
  const alertColors = await theme.getByRole("alert").evaluateAll((alerts) => {
    const parseRGB = (value: string): [number, number, number] => {
      const channels = value.match(/\d+(?:\.\d+)?/g);
      if (!channels || channels.length < 3)
        throw new Error(`expected RGB color, got ${value}`);
      return [Number(channels[0]), Number(channels[1]), Number(channels[2])];
    };
    const luminance = (rgb: [number, number, number]) =>
      rgb.reduce((sum, channel, index) => {
        const normalized = channel / 255;
        const linear =
          normalized <= 0.04045
            ? normalized / 12.92
            : ((normalized + 0.055) / 1.055) ** 2.4;
        return sum + linear * [0.2126, 0.7152, 0.0722][index];
      }, 0);
    const contrast = (foreground: string, background: string) => {
      const first = luminance(parseRGB(foreground));
      const second = luminance(parseRGB(background));
      return (
        (Math.max(first, second) + 0.05) / (Math.min(first, second) + 0.05)
      );
    };
    const themeBackground = alerts[0]?.closest<HTMLElement>(
      ".theme-light, .theme-dark",
    );
    const pageBackground = getComputedStyle(
      themeBackground || document.body,
    ).backgroundColor;
    return alerts.map((alert) => {
      const styles = getComputedStyle(alert);
      const text = alert.querySelector<HTMLElement>("strong") || alert;
      return {
        background: styles.backgroundColor,
        border: styles.borderTopColor,
        foreground: getComputedStyle(text).color,
        textContrast: contrast(
          getComputedStyle(text).color,
          styles.backgroundColor,
        ),
        boundaryContrast: contrast(styles.borderTopColor, pageBackground),
      };
    });
  });
  for (const alert of alertColors) {
    if (alert.textContrast < 4.5 || alert.boundaryContrast < 3) {
      throw new Error(
        `insufficient lifecycle warning contrast: ${JSON.stringify(alert)}`,
      );
    }
  }
  const assertWarningBoundaries = async () => {
    const boundaries = await theme
      .locator('[role="alert"], .fork-lifecycle-warning-error')
      .evaluateAll((nodes) => {
        const parseRGB = (value: string): [number, number, number] => {
          const channels = value.match(/\d+(?:\.\d+)?/g);
          if (!channels || channels.length < 3)
            throw new Error(`expected RGB color, got ${value}`);
          return [
            Number(channels[0]),
            Number(channels[1]),
            Number(channels[2]),
          ];
        };
        const luminance = (rgb: [number, number, number]) => {
          const linear = rgb.map((channel) => {
            const normalized = channel / 255;
            return normalized <= 0.04045
              ? normalized / 12.92
              : ((normalized + 0.055) / 1.055) ** 2.4;
          });
          return linear[0] * 0.2126 + linear[1] * 0.7152 + linear[2] * 0.0722;
        };
        const contrast = (foreground: string, background: string) => {
          const first = luminance(parseRGB(foreground));
          const second = luminance(parseRGB(background));
          return (
            (Math.max(first, second) + 0.05) / (Math.min(first, second) + 0.05)
          );
        };
        return nodes.flatMap((node) => {
          const styles = getComputedStyle(node);
          const page = node.closest<HTMLElement>(".theme-light, .theme-dark");
          const pageBackground = getComputedStyle(
            page || document.body,
          ).backgroundColor;
          const textNodes = [
            node,
            ...Array.from(node.querySelectorAll<HTMLElement>("strong, .small")),
          ];
          return textNodes.map((textNode) => ({
            textContrast: contrast(
              getComputedStyle(textNode).color,
              styles.backgroundColor,
            ),
            borderContrast: contrast(styles.borderTopColor, pageBackground),
            borderStyle: styles.borderTopStyle,
            borderWidth: styles.borderTopWidth,
          }));
        });
      });
    for (const boundary of boundaries) {
      expect(boundary.textContrast).toBeGreaterThanOrEqual(4.5);
      expect(boundary.borderContrast).toBeGreaterThanOrEqual(3);
      expect(boundary.borderStyle).not.toBe("none");
      expect(Number.parseFloat(boundary.borderWidth)).toBeGreaterThan(0);
    }
  };
  const assertButtonBoundaries = async (state: string, target: string | null = null) => {
    if (!state.startsWith("hover-") && !state.startsWith("active-")) {
      await page.mouse.move(0, 0);
    }
    const buttons = await theme.locator("button, input").evaluateAll((nodes) =>
      nodes.map((node) => {
        const styles = getComputedStyle(node);
        type RGBA = [number, number, number, number];
        const parseRGBA = (value: string): RGBA => {
          if (value === "transparent") return [0, 0, 0, 0];
          const channels = value.match(/\d+(?:\.\d+)?/g);
          if (!channels || channels.length < 3) throw new Error(`expected RGB color, got ${value}`);
          return [Number(channels[0]), Number(channels[1]), Number(channels[2]), channels[3] === undefined ? 1 : Number(channels[3])];
        };
        const composite = (foreground: RGBA, background: RGBA): RGBA => {
          const alpha = foreground[3] + background[3] * (1 - foreground[3]);
          if (alpha === 0) return [0, 0, 0, 0];
          return [0, 1, 2].map((index) =>
            (foreground[index] * foreground[3] + background[index] * background[3] * (1 - foreground[3])) / alpha,
          ).concat(alpha) as RGBA;
        };
        const mixOpacity = (foreground: RGBA, background: RGBA, opacity: number): RGBA =>
          composite([foreground[0], foreground[1], foreground[2], foreground[3] * opacity], background);
        const backgroundBehind = (element: Element): RGBA => {
          const layers: RGBA[] = [];
          for (let current = element.parentElement; current; current = current.parentElement) {
            layers.push(parseRGBA(getComputedStyle(current).backgroundColor));
          }
          return layers.reverse().reduce((background, layer) => composite(layer, background), [255, 255, 255, 1] as RGBA);
        };
        const luminance = (rgba: RGBA) => {
          const linear = rgba.slice(0, 3).map((channel) => {
            const normalized = channel / 255;
            return normalized <= 0.04045 ? normalized / 12.92 : ((normalized + 0.055) / 1.055) ** 2.4;
          });
          return linear[0] * 0.2126 + linear[1] * 0.7152 + linear[2] * 0.0722;
        };
        const contrast = (first: RGBA, second: RGBA) =>
          (Math.max(luminance(first), luminance(second)) + 0.05) /
          (Math.min(luminance(first), luminance(second)) + 0.05);
        const adjacent = backgroundBehind(node);
        const opacity = Number(styles.opacity);
        const opaqueBackground = composite(parseRGBA(styles.backgroundColor), adjacent);
        const background = mixOpacity(opaqueBackground, adjacent, opacity);
        const foreground = mixOpacity(composite(parseRGBA(styles.color), opaqueBackground), adjacent, opacity);
        const border = mixOpacity(composite(parseRGBA(styles.borderTopColor), opaqueBackground), adjacent, opacity);
        const outline = composite(parseRGBA(styles.outlineColor), adjacent);
        const focusVisible = node.matches(":focus-visible");
        return {
          label: node instanceof HTMLInputElement ? `input:${node.value}` : node.textContent?.trim(),
          className: node.className,
          disabled: node instanceof HTMLButtonElement || node instanceof HTMLInputElement ? node.disabled : false,
          pseudoDisabled: node.matches(":disabled"),
          cursor: styles.cursor,
          color: styles.color,
          background: styles.backgroundColor,
          adjacentBackground: adjacent.slice(0, 3).map((value) => Math.round(value)),
          compositedBackground: background.slice(0, 3).map((value) => Math.round(value)),
          borderColor: styles.borderTopColor,
          borderStyle: styles.borderTopStyle,
          borderWidth: styles.borderTopWidth,
          outlineColor: focusVisible ? styles.outlineColor : "rgba(0, 0, 0, 0)",
          outlineWidth: focusVisible ? styles.outlineWidth : "0px",
          opacity: styles.opacity,
          pseudoHover: node.matches(":hover"),
          pseudoActive: node.matches(":active"),
          pseudoFocusVisible: focusVisible,
          textContrast: contrast(foreground, background),
          borderContrast: contrast(border, adjacent),
          focusRingRequired: focusVisible,
          focusRingContrast: focusVisible && styles.outlineStyle !== "none" ? contrast(outline, adjacent) : 0,
        };
      }),
    );
    for (const button of buttons) {
      expect(button.color).not.toBe(button.background);
      expect(button.borderStyle).not.toBe("none");
      expect(Number.parseFloat(button.borderWidth)).toBeGreaterThan(0);
      expect(Number(button.opacity)).toBeGreaterThan(0);
      expect(button.textContrast, JSON.stringify(button)).toBeGreaterThanOrEqual(4.5);
      expect(button.borderContrast, JSON.stringify(button)).toBeGreaterThanOrEqual(3);
      if (button.focusRingRequired) expect(button.focusRingContrast, JSON.stringify(button)).toBeGreaterThanOrEqual(3);
      if (button.disabled) expect(button.cursor).toBe("not-allowed");
    }
    const isTarget = (control: (typeof buttons)[number]) =>
        target === "input"
          ? control.label?.startsWith("input:")
          : target !== null && control.label?.toLowerCase().includes(target);
    if (target !== null) {
      const targetControl = buttons.find(isTarget);
      expect(targetControl, `${state} target ${target}`).toBeDefined();
      if (state.startsWith("focus-")) expect(targetControl?.pseudoFocusVisible).toBe(true);
      if (state.startsWith("hover-")) expect(targetControl?.pseudoHover).toBe(true);
      if (state.startsWith("active-")) expect(targetControl?.pseudoActive).toBe(true);
    }
    visualEvidence.push({
      state,
      target,
      controls: buttons.map((button) => ({
        ...button,
        target: isTarget(button),
        semantic: button.disabled
          ? "unavailable"
          : button.className.includes("btn-danger")
            ? "destructive"
            : button.className.includes("btn-primary")
              ? "primary"
              : button.className.includes("btn-secondary")
                ? "neutral"
                : "input",
      })),
    });
    return buttons;
  };
  await assertWarningBoundaries();
  const restingControls = await assertButtonBoundaries("resting");
  const overflow = await theme.evaluate(
    (node) => node.scrollWidth <= node.clientWidth,
  );
  expect(overflow).toBe(true);
  await expect(theme).toHaveScreenshot(`ForkLifecycleWarning-${themeName}-resting.png`);
  const assertNoOverflow = async () => {
    expect(
      await theme.evaluate((node) => node.scrollWidth <= node.clientWidth),
    ).toBe(true);
  };

  await loadState("confirmation");
  let abandon = theme.getByRole("button", { name: "Abandon" });
  await abandon.click();
  await expect(theme.getByLabel("Abandon reason")).toBeVisible();
  await expect(
    theme.getByRole("button", { name: "Confirm abandon" }),
  ).toBeDisabled();
  await assertWarningBoundaries();
  const disabledConfirmationControls = await assertButtonBoundaries("confirmation-disabled", "confirm");
  const disabledConfirm = disabledConfirmationControls.find((control) => control.label === "Confirm abandon");
  expect(disabledConfirm?.disabled).toBe(true);
  await expect(theme).toHaveScreenshot(
    `ForkLifecycleWarning-${themeName}-confirmation.png`,
  );
  await theme.getByLabel("Abandon reason").fill("operator-approved reason");
  const populatedConfirmationControls = await assertButtonBoundaries("confirmation-enabled", "confirm");
  const enabledConfirm = populatedConfirmationControls.find((control) => control.label === "Confirm abandon");
  expect(enabledConfirm?.disabled).toBe(false);
  expect(enabledConfirm?.background, JSON.stringify({ disabledConfirm, enabledConfirm })).not.toBe(disabledConfirm?.background);
  expect(enabledConfirm?.borderStyle).not.toBe(disabledConfirm?.borderStyle);
  await expect(theme).toHaveScreenshot(`ForkLifecycleWarning-${themeName}-confirmation-populated.png`);
  for (const [name, control] of [
    ["input", theme.getByLabel("Abandon reason")],
    ["confirm", theme.getByRole("button", { name: "Confirm abandon" })],
    ["cancel", theme.getByRole("button", { name: "Cancel" })],
  ] as const) {
    await page.keyboard.press("Tab");
    await control.focus();
    await expect(control).toBeFocused();
    await assertButtonBoundaries(name === "cancel" ? "focus-cancel-confirmation" : `focus-${name}`, name);
    await expect(theme).toHaveScreenshot(`ForkLifecycleWarning-${themeName}-focus-${name}.png`);
    await control.hover();
    await assertButtonBoundaries(`hover-${name}`, name);
    await expect(theme).toHaveScreenshot(`ForkLifecycleWarning-${themeName}-hover-${name}.png`);
    const box = await control.boundingBox();
    if (!box) throw new Error(`missing ${name} control bounds`);
    await page.mouse.move(box.x + box.width / 2, box.y + box.height / 2);
    await page.mouse.down();
    await assertButtonBoundaries(`active-${name}`, name);
    await expect(theme).toHaveScreenshot(`ForkLifecycleWarning-${themeName}-active-${name}.png`);
    await page.mouse.move(0, 0);
    await page.mouse.up();
  }
  await assertNoOverflow();
  await loadState("cancel");
  abandon = theme.getByRole("button", { name: "Abandon" });
  await abandon.click();
  const cancel = theme.getByRole("button", { name: "Cancel" });
  await page.keyboard.press("Tab");
  await cancel.focus();
  await assertWarningBoundaries();
  await assertButtonBoundaries("focus-cancel-dismiss", "cancel");
  await expect(theme).toHaveScreenshot(
    `ForkLifecycleWarning-${themeName}-cancel.png`,
  );
  await cancel.click();
  await expect(theme.getByLabel("Abandon reason")).toHaveCount(0);
  await assertNoOverflow();

  await loadState("focus");
  let retry = theme.getByRole("button", { name: "Retry" });
  await page.keyboard.press("Tab");
  await retry.focus();
  await expect(retry).toBeFocused();
  await assertWarningBoundaries();
  await assertButtonBoundaries("focus-retry", "retry");
  await expect(theme).toHaveScreenshot(
    `ForkLifecycleWarning-${themeName}-focus.png`,
  );
  await assertNoOverflow();
  const focusAbandon = theme.getByRole("button", { name: "Abandon" });
  await page.keyboard.press("Tab");
  await focusAbandon.focus();
  await expect(focusAbandon).toBeFocused();
  await assertWarningBoundaries();
  await assertButtonBoundaries("focus-abandon", "abandon");
  await expect(theme).toHaveScreenshot(`ForkLifecycleWarning-${themeName}-focus-abandon.png`);
  await loadState("hover");
  retry = theme.getByRole("button", { name: "Retry" });
  await retry.hover();
  await assertWarningBoundaries();
  await assertButtonBoundaries("hover-retry", "retry");
  await expect(theme).toHaveScreenshot(
    `ForkLifecycleWarning-${themeName}-resting-hover.png`,
  );
  await assertNoOverflow();
  const hoverAbandon = theme.getByRole("button", { name: "Abandon" });
  await hoverAbandon.hover();
  await assertWarningBoundaries();
  await assertButtonBoundaries("hover-abandon", "abandon");
  await expect(theme).toHaveScreenshot(`ForkLifecycleWarning-${themeName}-hover-abandon.png`);
  for (const [name, control] of [
    ["retry", retry],
    ["abandon", hoverAbandon],
  ] as const) {
    const box = await control.boundingBox();
    if (!box) throw new Error(`missing ${name} control bounds`);
    await page.mouse.move(box.x + box.width / 2, box.y + box.height / 2);
    await page.mouse.down();
    await assertButtonBoundaries(`active-${name}`, name);
    await expect(theme).toHaveScreenshot(`ForkLifecycleWarning-${themeName}-active-${name}.png`);
    await page.mouse.move(0, 0);
    await page.mouse.up();
  }
  await loadState("loading");
  retry = theme.getByRole("button", { name: "Retry" });
  await retry.click();
  await expect(
    theme.getByRole("button", { name: "Retrying..." }),
  ).toBeDisabled();
  await assertWarningBoundaries();
  const retryLoadingControls = await assertButtonBoundaries("loading-retry", "retry");
  const restingRetry = restingControls.find((control) => control.label === "Retry");
  const disabledRetry = retryLoadingControls.find((control) => control.label === "Retrying...");
  const restingAbandon = restingControls.find((control) => control.label === "Abandon");
  const disabledAbandon = retryLoadingControls.find((control) => control.label === "Abandon");
  expect(disabledRetry?.background).not.toBe(restingRetry?.background);
  expect(disabledAbandon?.background).not.toBe(restingAbandon?.background);
  expect(disabledRetry?.borderStyle).toBe("dashed");
  expect(disabledAbandon?.borderStyle).toBe("dashed");
  await expect(theme).toHaveScreenshot(
    `ForkLifecycleWarning-${themeName}-loading.png`,
  );
  await assertNoOverflow();
  await page.evaluate(() => window.dispatchEvent(new Event("fork-warning-release")));
  await expect(theme.getByRole("button", { name: "Retry" })).toBeVisible();

  await loadState("abandon-loading");
  await theme.getByRole("button", { name: "Abandon" }).click();
  await theme.getByLabel("Abandon reason").fill("operator-approved reason");
  await theme.getByRole("button", { name: "Confirm abandon" }).click();
  await expect(theme.getByRole("button", { name: "Abandoning..." })).toBeDisabled();
  await expect(theme.getByLabel("Abandon reason")).toBeDisabled();
  await expect(theme.getByRole("button", { name: "Cancel" })).toBeDisabled();
  const abandonLoadingControls = await assertButtonBoundaries("loading-abandon", "abandon");
  const disabledConfirmLoading = abandonLoadingControls.find((control) => control.label === "Abandoning...");
  const disabledCancel = abandonLoadingControls.find((control) => control.label === "Cancel");
  const enabledCancel = populatedConfirmationControls.find((control) => control.label === "Cancel");
  expect(disabledConfirmLoading?.background).not.toBe(enabledConfirm?.background);
  expect(disabledCancel?.background).not.toBe(enabledCancel?.background);
  expect(disabledConfirmLoading?.borderStyle).toBe("dashed");
  expect(disabledCancel?.borderStyle).toBe("dashed");
  await assertWarningBoundaries();
  await expect(theme).toHaveScreenshot(`ForkLifecycleWarning-${themeName}-abandon-loading.png`);
  await page.evaluate(() => window.dispatchEvent(new Event("fork-warning-release")));

  await loadState("action-error");
  await theme.getByRole("button", { name: "Retry" }).click();
  await expect(page.getByText("The queue changed on the server. The warning was refreshed.")).toBeVisible();
  await assertWarningBoundaries();
  await assertButtonBoundaries("action-error");
  await expect(theme).toHaveScreenshot(`ForkLifecycleWarning-${themeName}-error.png`);
  await assertNoOverflow();

  await loadState("refresh-error");
  await theme.getByRole("button", { name: "Retry" }).click();
  await expect(page.getByText(/Warning refresh failed: 500/)).toBeVisible();
  await assertWarningBoundaries();
  await assertButtonBoundaries("refresh-error");
  await expect(theme).toHaveScreenshot(`ForkLifecycleWarning-${themeName}-refresh-error.png`);

  await loadState("overflow");
  await assertWarningBoundaries();
  await assertButtonBoundaries("overflow");
  await assertNoOverflow();
  await expect(theme).toHaveScreenshot(`ForkLifecycleWarning-${themeName}-overflow.png`);

  await loadState("success");
  await theme.getByRole("button", { name: "Retry" }).click();
  await expect(theme.getByRole("alert")).toHaveCount(0);
  await assertButtonBoundaries("success");
  await assertNoOverflow();
  await expect(theme).toHaveScreenshot(
    `ForkLifecycleWarning-${themeName}-success.png`,
  );
  expect(JSON.stringify({ theme: themeName, states: visualEvidence }, null, 2)).toMatchSnapshot(
    `ForkLifecycleWarning-${themeName}-rendered-colors.json`,
  );
};

for (const visualState of ["warning-only", "blocked-only"] as const) {
  test(`Fork lifecycle warning ${visualState} state`, async ({ page }) => {
    test.skip(
      ["webkit", "iphone"].includes(test.info().project.name),
      "Problem 4 warning visual baselines are scoped to installed Chromium, Firefox, and Android projects",
    );
    await page.clock.install({ time: new Date("2018-08-14T17:00:00Z") });
    await page.goto("/src/e2e/stories.html");
    await page.evaluate((state) => sessionStorage.setItem("forkLifecycleWarningVisualState", state), visualState);
    await page.goto(`/src/e2e/stories.html#ForkLifecycleWarning`);
    await page.reload();
    await page.locator('[role="alert"]').first().waitFor();
    const alerts = page.locator('[role="alert"]');
    const kinds = await page.locator("[data-issue-kind]").evaluateAll((nodes) =>
      nodes.map((node) => node.getAttribute("data-issue-kind")).sort(),
    );
    if (visualState === "warning-only") {
      await expect(alerts).toHaveCount(8);
      await expect(page.locator(".alert-danger")).toHaveCount(0);
      await expect(page.getByRole("button", { name: "Retry" })).toHaveCount(0);
      expect(kinds).toEqual([
        "recheck_retrying", "recheck_retrying",
        "regroup_in_progress", "regroup_in_progress",
        "regroup_out_of_date", "regroup_out_of_date",
        "silence_ha_inconsistent", "silence_ha_inconsistent",
      ]);
    } else {
      await expect(alerts).toHaveCount(2);
      await expect(page.locator(".alert-warning")).toHaveCount(0);
      await expect(page.getByRole("button", { name: "Retry" })).toHaveCount(2);
      expect(kinds).toEqual(["recheck_blocked", "recheck_blocked"]);
    }
    await measureProblem4WarningState(
      page,
      visualState,
      `ForkLifecycleWarning-${visualState}-rendered-colors.json`,
    );
    await expect(page).toHaveScreenshot(
      `ForkLifecycleWarning-${visualState}.png`,
      { fullPage: true },
    );
  });
}

test("Fork lifecycle warning independent states are visually distinct", async ({ page }) => {
  test.skip(
    ["webkit", "iphone"].includes(test.info().project.name),
    "Problem 4 warning visual baselines are scoped to installed Chromium, Firefox, and Android projects",
  );
  await page.goto("/src/e2e/stories.html");
  const capture = async (state: string) => {
    await page.evaluate((value) => sessionStorage.setItem("forkLifecycleWarningVisualState", value), state);
    await page.goto("/src/e2e/stories.html#ForkLifecycleWarning");
    await page.reload();
    await page.locator('[role="alert"]').first().waitFor();
    await measureProblem4WarningState(
      page,
      `distinction-${state}`,
      `ForkLifecycleWarning-distinction-${state}-rendered-colors.json`,
    );
    return page.locator("#root").screenshot();
  };
  const states = [];
  for (const state of ["resting", "warning-only", "blocked-only"]) {
    states.push(await capture(state));
  }
  for (let first = 0; first < states.length; first += 1) {
    for (let second = first + 1; second < states.length; second += 1) {
      expect(states[first].equals(states[second])).toBe(false);
    }
  }
});

for (const themeName of ["light", "dark"] as const) {
  test(`Fork lifecycle warning ${themeName} interactive states`, async ({
    page,
  }) => {
    test.skip(
      ["webkit", "iphone"].includes(test.info().project.name),
      "Problem 4 warning visual baselines are scoped to installed Chromium, Firefox, and Android projects",
    );
    await runForkLifecycleWarningInteractiveVisual(page, themeName);
  });
}

test("SilenceModalEditorDraftPreview", async ({ page }) => {
  await page.clock.install({ time: new Date("2018-08-14T17:00:00Z") });
  await page.goto("/src/e2e/stories.html#SilenceModalEditor");
  await page.addStyleTag({
    content: "* { caret-color: transparent !important; }",
  });

  const editors = page.locator(".fork-silence-editor");
  const count = await editors.count();
  const previews = page.locator(".fork-silence-editor__preview");
  for (let index = 0; index < count; index++) {
    const matcher = editors
      .nth(index)
      .locator(".fork-silence-editor__matcher-expression")
      .first();
    await matcher.click();
    await matcher.fill('service="api"');
    await page.clock.runFor(500);
    const preview = previews.nth(index);
    await expect(
      preview.getByText("Preview excludes matcher being edited"),
    ).toBeVisible();
    await expect(
      preview.locator(".fork-silence-editor__preview-label--draft-match"),
    ).toHaveCount(1);
    await expect(
      preview.locator(".fork-silence-editor__preview-label--draft-no-match"),
    ).toHaveCount(2);
    const draftContrasts = await preview.evaluate((node) => {
      const rgb = (color: string): [number, number, number] => {
        const channels = color.match(/\d+/g);
        if (!channels || channels.length < 3) {
          throw new Error(`expected RGB color, got ${color}`);
        }
        return [Number(channels[0]), Number(channels[1]), Number(channels[2])];
      };
      const luminance = ([red, green, blue]: [number, number, number]) => {
        const linear = [red, green, blue].map((channel) => {
          const normalized = channel / 255;
          return normalized <= 0.04045
            ? normalized / 12.92
            : ((normalized + 0.055) / 1.055) ** 2.4;
        });
        return linear[0] * 0.2126 + linear[1] * 0.7152 + linear[2] * 0.0722;
      };
      const contrast = (foreground: string, background: string) => {
        const [first, second] = [
          luminance(rgb(foreground)),
          luminance(rgb(background)),
        ];
        return (
          (Math.max(first, second) + 0.05) / (Math.min(first, second) + 0.05)
        );
      };
      const measured = [
        ".fork-silence-editor__preview-label--draft-match",
        ".fork-silence-editor__preview-label--draft-no-match",
      ].map((selector) => {
        const label = node.querySelector<HTMLElement>(selector);
        if (!label) {
          throw new Error(`expected ${selector}`);
        }
        const styles = getComputedStyle(label);
        return [
          contrast(
            getComputedStyle(label.querySelector(".syntax-label")!).color,
            styles.backgroundColor,
          ),
          contrast(
            getComputedStyle(label.querySelector(".syntax-operator")!).color,
            styles.backgroundColor,
          ),
          contrast(
            getComputedStyle(label.querySelector(".syntax-value")!).color,
            styles.backgroundColor,
          ),
        ];
      });
      return measured.flat();
    });
    for (const contrast of draftContrasts) {
      expect(contrast).toBeGreaterThanOrEqual(4.5);
    }
    await expect(
      preview.locator(".fork-silence-editor__preview-label--draft-match"),
    ).toHaveScreenshot(`SilenceModalEditorDraftPreviewMatch-${index}.png`);
    await expect(
      preview
        .locator(".fork-silence-editor__preview-label--draft-no-match")
        .first(),
    ).toHaveScreenshot(`SilenceModalEditorDraftPreviewNoMatch-${index}.png`);
    await expect(preview).toHaveScreenshot(
      `SilenceModalEditorDraftPreview-${index}.png`,
    );
  }
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
      return (
        row.getBoundingClientRect().top - select.getBoundingClientRect().bottom
      );
    };
    return {
      startsDetailGap: timingGap(starts),
      endsDetailGap: timingGap(ends),
      previewGap:
        preview.getBoundingClientRect().top -
        grid.getBoundingClientRect().bottom,
    };
  });

  expect(measurements.startsDetailGap).toBeCloseTo(
    measurements.endsDetailGap,
    0,
  );
  expect(measurements.previewGap).toBeGreaterThanOrEqual(11);
});

test("SilenceModalEditorValidationLayoutIsStable", async ({ page }) => {
  await page.goto("/src/e2e/stories.html#SilenceModalEditorValidation");

  const editor = page.locator(".fork-silence-editor").first();
  const matcher = editor.locator(".fork-silence-editor__matcher-control");
  const preview = editor.locator(".fork-silence-editor__preview");
  const before = {
    matcher: await matcher.boundingBox(),
    preview: await preview.boundingBox(),
  };
  await editor.getByLabel("Author").fill("operator@example.com");
  await editor.getByLabel("Comment").fill("Planned maintenance");
  const after = {
    matcher: await matcher.boundingBox(),
    preview: await preview.boundingBox(),
  };

  expect(before.matcher?.y).toBe(after.matcher?.y);
  expect(before.preview?.y).toBe(after.preview?.y);
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
      expected.style.color = "var(--karma-silence-editor-focus-border)";
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

  const matcherControls = page.locator(".fork-silence-editor__matcher-control");
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
    content: `
      * { caret-color: transparent !important; }
      .fork-silence-editor__matcher-control { height: 36px !important; }
      .fork-silence-editor__matcher-expression { line-height: 36px !important; }
    `,
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
    // WebKit can antialias a few syntax glyph edge pixels differently.
    await expect(controls.nth(i)).toHaveScreenshot(
      `SilenceModalEditorMatcherSyntax-${i}.png`,
      { maxDiffPixels: 10, maxDiffPixelRatio: 0.001 },
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
    await page.clock.runFor(250);
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
