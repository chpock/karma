import corpus from "../../../../fork-docs/SPEC/testdata/alertmanager-matcher-corpus.json";

import { parseMatcherExpression } from "./SilenceEditor";

describe("Alertmanager matcher corpus", () => {
  it("matches every canonical Alertmanager parser case", () => {
    for (const testCase of corpus) {
      const parsed = parseMatcherExpression(testCase.expression);
      const expected = testCase.valid
        ? {
            name: testCase.name,
            value: testCase.value,
            isRegex: testCase.isRegex,
            isEqual: testCase.isEqual,
          }
        : null;
      expect({ expression: testCase.expression, parsed }).toEqual({
        expression: testCase.expression,
        parsed: expected,
      });
    }
  });
});
