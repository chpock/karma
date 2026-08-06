import { FC, useCallback, MouseEvent } from "react";

import { observer } from "mobx-react-lite";

import { CSSTransition } from "react-transition-group";

import type { AlertStore } from "Stores/AlertStore";
import { QueryOperators, FormatQuery } from "Common/Query";
import { TooltipWrapper } from "Components/TooltipWrapper";
import { GetClassAndStyle } from "Components/Labels/Utils";
import { useFlashTransition } from "Hooks/useFlashTransition";

// Same as FilteringLabel but for labels that are counters (usually @state)
// and only renders a pill badge with the counter, it doesn't render anything
// if the counter is 0
const FilteringCounterBadge: FC<{
  alertStore: AlertStore;
  name: string;
  value: string;
  filterValue?: string;
  counter: number;
  themed: boolean;
  alwaysVisible?: boolean;
  defaultColor?: "bg-light" | "bg-primary";
  isAppend?: boolean;
  appendFilters?: string[];
  replaceWith?: string;
}> = ({
  alertStore,
  name,
  value,
  filterValue,
  counter,
  themed,
  alwaysVisible = false,
  defaultColor = "bg-light",
  isAppend = true,
  appendFilters = [],
  replaceWith,
}) => {
  const { ref, props } = useFlashTransition(counter);

  const handleClick = useCallback(
    (event: MouseEvent) => {
      // left click       => apply foo=bar filter
      // left click + alt => apply foo!=bar filter
      let operators = [QueryOperators.Equal, QueryOperators.NotEqual];
      if (event.altKey) {
        operators = operators.reverse();
      }

      event.preventDefault();

      const stateValue = filterValue || value;
      const positive = replaceWith || FormatQuery(name, QueryOperators.Equal, stateValue);
      const negative = FormatQuery(name, QueryOperators.NotEqual, stateValue);
      const isPositive = operators[0] === QueryOperators.Equal;
      const selected = isPositive ? positive : negative;

      if (isAppend) {
        alertStore.filters.replaceFilter(negative, selected);
        if (isPositive) {
          for (const raw of appendFilters) {
            alertStore.filters.addFilter(raw);
          }
        }
      } else {
        const raws = [selected];
        if (isPositive) {
          for (const raw of appendFilters) {
            if (!raws.includes(raw)) {
              raws.push(raw);
            }
          }
        }
        alertStore.filters.setFilters(raws);
      }
    },
    [alertStore.filters, name, value, isAppend, appendFilters, replaceWith],
  );

  const cs = GetClassAndStyle(
    alertStore,
    name,
    value,
    "rounded-pill components-label-with-hover",
  );

  if (!alwaysVisible && counter === 0) return null;

  return (
    <TooltipWrapper
      title={`Click to only show ${name}=${value} alerts or Alt+Click to hide them`}
    >
      <CSSTransition {...props}>
        <span
          ref={ref}
          className={
            themed
              ? cs.className
              : [
                  `${defaultColor}`,
                  "rounded-pill components-label-with-hover",
                  ...cs.baseClassNames,
                ].join(" ")
          }
          style={themed ? {} : cs.style}
          onClick={handleClick}
        >
          {counter}
        </span>
      </CSSTransition>
    </TooltipWrapper>
  );
};

export default observer(FilteringCounterBadge);
