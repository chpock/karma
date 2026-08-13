import { mergeDateTime } from "./SilenceEditor";

const timezone = Intl.DateTimeFormat().resolvedOptions().timeZone;
const describeInNewYork =
  timezone === "America/New_York" ? describe : describe.skip;

describeInNewYork("absolute calendar input in America/New_York", () => {
  it("rejects the nonexistent spring-forward wall time", () => {
    expect(mergeDateTime("2026-03-08", "02:30")).toBeNull();
  });

  it("selects and exposes the earlier offset for ambiguous fall-back time", () => {
    const date = mergeDateTime("2026-11-01", "01:30");
    expect(date?.toISOString()).toBe("2026-11-01T05:30:00.000Z");
    expect(date?.getTimezoneOffset()).toBe(240);
  });
});
