import type { FC } from "react";

import type { APIAlertSuppressionCauseT } from "Models/APITypes";
import { MatcherToExpression } from "Stores/SilenceFormStore";

const SuppressionEvidence: FC<{
  causes: APIAlertSuppressionCauseT[];
}> = ({ causes }) => {
  if (causes.length === 0) return null;

  return (
    <div className="fork-suppression-evidence mt-1">
      <div className="text-muted small mb-1">Retained suppression evidence</div>
      {causes.map((cause) => (
        <div
          className="border-start border-secondary ps-2 mb-1 small"
          key={`${cause.target}/${cause.source}/${cause.silenceID}`}
        >
          <div>
            <code>{cause.silenceID}</code>{" "}
            <span className="text-muted">
              {cause.target} / {cause.source}
            </span>
          </div>
          {cause.resolutionStatus === "known" ? (
            <>
              <div className="text-break">{cause.comment}</div>
              <div className="d-flex flex-wrap gap-1">
                {cause.matchers.map((matcher, index) => (
                  <code
                    key={`${index}/${matcher.name}/${matcher.value}/${matcher.isRegex}/${matcher.isEqual}`}
                  >
                    {MatcherToExpression(matcher)}
                  </code>
                ))}
              </div>
              <div className="text-muted">
                {cause.createdBy ? `Author: ${cause.createdBy}. ` : null}
                {cause.startsAt && cause.endsAt
                  ? `${cause.startsAt} to ${cause.endsAt}`
                  : null}
              </div>
            </>
          ) : (
            <div className="text-muted">Rule details were not available.</div>
          )}
        </div>
      ))}
    </div>
  );
};

export { SuppressionEvidence };
