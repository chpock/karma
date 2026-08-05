// fallback class for labels
const DefaultLabelClassMap = Object.freeze({
  badge: "bg-default components-label-dark",
  btn: "btn-default components-label-dark",
});

// same but for borders
const BorderClassMap = Object.freeze({
  active: "border-danger",
  suppressed: "border-success",
  unprocessed: "border-secondary",
  inactive: "border-inactive",
  resolved: "border-resolved",
});

const BackgroundClassMap = Object.freeze({
  active: "bg-danger",
  suppressed: "bg-success",
  unprocessed: "bg-secondary",
  inactive: "bg-inactive",
  resolved: "bg-resolved",
});

const StaticColorLabelClassMap = Object.freeze({
  badge: "bg-info components-label-dark",
  btn: "btn-info components-label-dark",
});

const AlertNameLabelClassMap = Object.freeze({
  badge: "bg-dark components-label-dark",
  btn: "btn-dark components-label-dark",
});

const StateLabelClassMap = Object.freeze({
  active: "danger",
  suppressed: "success",
  unprocessed: "secondary",
  inactive: "inactive",
  resolved: "resolved",
});

export {
  DefaultLabelClassMap,
  StaticColorLabelClassMap,
  AlertNameLabelClassMap,
  BorderClassMap,
  BackgroundClassMap,
  StateLabelClassMap,
};
