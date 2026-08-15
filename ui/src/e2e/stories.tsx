import * as React from "react";
import { createRoot } from "react-dom/client";

import { addDays } from "date-fns/addDays";

import { faArrowUp } from "@fortawesome/free-solid-svg-icons/faArrowUp";
import { faExclamation } from "@fortawesome/free-solid-svg-icons/faExclamation";

import { ThemeContext } from "Components/Theme";
import type { ThemeCtx } from "Components/Theme";
import {
  ReactSelectColors,
  ReactSelectStyles,
} from "Components/Theme/ReactSelect";

import { AlertStore } from "Stores/AlertStore";
import { Settings } from "Stores/Settings";
import { SilenceFormStore } from "Stores/SilenceFormStore";

import { MockGrid, MockGroup } from "__fixtures__/Stories";
import { MockAlert, MockAlertGroup, MockSilence } from "__fixtures__/Alerts";

import { FatalError } from "Components/Grid/FatalError";
import { UpgradeNeeded } from "Components/Grid/UpgradeNeeded";
import { ReloadNeeded } from "Components/Grid/ReloadNeeded";
import { EmptyGrid } from "Components/Grid/EmptyGrid";
import { NoUpstream } from "Components/Grid/NoUpstream";
import Grid from "Components/Grid";
import AlertCard from "Components/Grid/AlertGrid/AlertGroup/Alert";
import NavBar from "Components/NavBar";
import { InternalError } from "ErrorBoundary";
import { MainModalContent } from "Components/MainModal/MainModalContent";
import { OverviewModalContent } from "Components/OverviewModal/OverviewModalContent";
import { ManagedSilence } from "Components/ManagedSilence";
import { SilenceModalContent } from "Components/SilenceModal/SilenceModalContent";
import { Toast } from "Components/Toast";
import {
  ToastMessage,
  UpgradeToastMessage,
} from "Components/Toast/ToastMessages";

import "Styles/E2E.scss";

const jsonResponse = (data: unknown): Response =>
  new Response(JSON.stringify(data), {
    status: 200,
    headers: { "Content-Type": "application/json" },
  });

const countersData = {
  total: 90,
  counters: [
    {
      name: "@receiver",
      hits: 2,
      values: [
        {
          value: "by-cluster-service",
          raw: "@receiver=by-cluster-service",
          hits: 2,
          percent: 100,
          offset: 0,
        },
      ],
    },
    {
      name: "alertname",
      hits: 90,
      values: [
        {
          value: "Fake Alert",
          raw: "alertname=Fake Alert",
          hits: 45,
          percent: 50,
          offset: 0,
        },
        {
          value: "Second Fake Alert",
          raw: "alertname=Second Fake Alert",
          hits: 45,
          percent: 50,
          offset: 50,
        },
      ],
    },
    {
      name: "group",
      hits: 100,
      values: [
        {
          value: "group1",
          raw: "group=group1",
          hits: 25,
          percent: 25,
          offset: 0,
        },
        {
          value: "group2",
          raw: "group=group2",
          hits: 70,
          percent: 70,
          offset: 25,
        },
        {
          value: "group3",
          raw: "group=group3",
          hits: 4,
          percent: 4,
          offset: 95,
        },
        {
          value: "group4",
          raw: "group=group4",
          hits: 1,
          percent: 1,
          offset: 99,
        },
      ],
    },
  ],
};

const originalFetch = window.fetch;
window.fetch = async (
  input: RequestInfo | URL,
  init?: RequestInit,
): Promise<Response> => {
  const url =
    typeof input === "string"
      ? input
      : input instanceof URL
        ? input.toString()
        : input.url;
  const story = window.location.hash.replace("#", "").split("?")[0];

  if (url.includes("/silence-editor/labels")) {
    return jsonResponse({
      status: "success",
      target: "am",
      total: 12,
      truncated: false,
      labels: [
        { name: "alertname", values: 6 },
        { name: "cluster", values: 2 },
        { name: "instance", values: 5 },
        { name: "service", values: 3 },
        { name: "deployment", values: 4 },
        { name: "environment", values: 3 },
        { name: "namespace", values: 6 },
        { name: "pod", values: 9 },
        { name: "prometheus", values: 2 },
        { name: "receiver", values: 4 },
        { name: "region", values: 3 },
        { name: "very_long_alert_label_name", values: 1 },
      ],
    });
  }

  if (url.includes("/silence-editor/values")) {
    const u = new URL(url, window.location.origin);
    const labelName = u.searchParams.get("name") || "";
    const values =
      labelName === "service"
        ? [
            { value: "api", hits: 11 },
            { value: "payments", hits: 7 },
            { value: "search", hits: 3 },
          ]
        : [
            { value: "prod", hits: 14 },
            { value: "staging", hits: 5 },
          ];
    return jsonResponse({
      status: "success",
      target: "am",
      name: labelName,
      total: values.length,
      truncated: false,
      values,
    });
  }

  if (url.includes("/silence-editor/preview")) {
    return jsonResponse({
      status: "success",
      target: "am",
      total: 3,
      returned: 3,
      limit: 100,
      excludedNoRawLabels: 0,
      alerts: [
        {
          id: "preview-a",
          state: "active",
          rawState: "active",
          isLive: true,
          isSuppressed: false,
          isResolved: false,
          startsAt: "2018-08-14T15:30:00Z",
          labels: [
            { name: "alertname", value: "Host_Down" },
            { name: "service", value: "api" },
            { name: "environment", value: "production" },
          ],
          matchedLabels: [
            { name: "alertname", value: "Host_Down" },
            { name: "service", value: "api" },
          ],
          matchedLabelNames: ["alertname", "service"],
          missingLabelNames: [],
        },
        {
          id: "preview-b",
          state: "suppressed",
          rawState: "suppressed",
          isLive: true,
          isSuppressed: true,
          isResolved: false,
          startsAt: "2018-08-14T14:05:00Z",
          labels: [
            { name: "alertname", value: "API_5xx" },
            { name: "service", value: "payments" },
          ],
          matchedLabels: [{ name: "service", value: "payments" }],
          matchedLabelNames: ["service"],
          missingLabelNames: ["cluster"],
        },
        {
          id: "preview-c",
          state: "inactive",
          rawState: "active",
          isLive: false,
          isSuppressed: false,
          isResolved: false,
          startsAt: "2018-08-13T09:00:00Z",
          lastSeenAt: "2018-08-14T16:58:00Z",
          labels: [
            { name: "alertname", value: "Disk_Full" },
            { name: "instance", value: "db01" },
          ],
          matchedLabels: [{ name: "instance", value: "db01" }],
          matchedLabelNames: ["instance"],
          missingLabelNames: [],
        },
      ],
    });
  }

  if (url.includes("/silence-editor/submit")) {
    return jsonResponse({
      status: "success",
      target: "am",
      mode: "proxy",
      submittedBy: "storybook",
      silenceID: "silence-storybook",
    });
  }

  if (url.includes("labelNames.json"))
    return jsonResponse(["cluster", "job", "instance"]);
  if (url.includes("labelValues.json"))
    return jsonResponse(["dev", "staging", "prod"]);
  if (url.includes("alertList.json")) return jsonResponse({ alerts: [] });
  if (url.includes("autocomplete.json")) return jsonResponse([]);
  if (url.includes("alerts.json"))
    return jsonResponse({ alerts: [], totalAlerts: 0 });

  if (url.includes("counters.json")) {
    if (story === "OverviewModal") return jsonResponse(countersData);
    return jsonResponse({ total: 0, counters: [] });
  }

  if (url.includes("silences.json")) return jsonResponse([]);

  return originalFetch(input, init);
};

type StoryMap = Record<string, () => React.ReactNode>;

const Modal: React.FC<{ children: React.ReactNode }> = ({
  children,
}: {
  children: React.ReactNode;
}) => (
  <div className="modal d-block" style={{ position: "relative" }}>
    <div className="modal-dialog modal-lg" role="document">
      <div className="modal-content">{children}</div>
    </div>
  </div>
);

const makeAlertStore = (): AlertStore => {
  const alertStore = new AlertStore([]);
  alertStore.data.setUpstreams({
    counters: { total: 1, healthy: 1, failed: 0 },
    instances: [
      {
        name: "am1",
        cluster: "am",
        clusterMembers: ["am1"],
        uri: "http://localhost:9093",
        publicURI: "http://example.com",
        readonly: false,
        error: "",
        warnings: [],
        version: "0.24.0",
        headers: {},
        corsCredentials: "include",
      },
    ],
    clusters: { am: ["am1"] },
  });
  return alertStore;
};

const makeReadOnlyAlertStore = (): AlertStore => {
  const alertStore = new AlertStore([]);
  alertStore.data.setUpstreams({
    counters: { healthy: 1, failed: 0, total: 1 },
    clusters: { ro: ["readonly"] },
    instances: [
      {
        name: "readonly",
        uri: "http://localhost:8080",
        publicURI: "http://example.com",
        readonly: true,
        headers: {},
        corsCredentials: "include",
        error: "",
        warnings: [],
        version: "0.24.0",
        cluster: "ro",
        clusterMembers: ["readonly"],
      },
    ],
  });
  return alertStore;
};

const makeGridAlertStore = (): AlertStore => {
  const alertStore = new AlertStore([]);
  MockGrid(alertStore);

  const suppressedGroup = MockGroup("suppressed-only", 3, 0, 3);
  suppressedGroup.id = "id-suppressed";
  suppressedGroup.stateCount = { active: 0, suppressed: 3, unprocessed: 0 };
  suppressedGroup.totalAlerts = 3;

  const unprocessedGroup = MockGroup("unprocessed-only", 3, 0, 0);
  unprocessedGroup.id = "id-unprocessed";
  unprocessedGroup.stateCount = { active: 0, suppressed: 0, unprocessed: 3 };
  unprocessedGroup.totalAlerts = 3;

  const grids = alertStore.data.grids;
  grids[0].alertGroups = [
    suppressedGroup,
    unprocessedGroup,
    ...grids[0].alertGroups,
  ];
  grids[0].totalGroups = grids[0].alertGroups.length;
  alertStore.data.setGrids(grids);

  return alertStore;
};

const gridStory = (): React.ReactNode => {
  const alertStore = makeGridAlertStore();
  const settingsStore = new Settings(null);
  settingsStore.alertGroupConfig.setDefaultCollapseState("expanded");
  const silenceFormStore = new SilenceFormStore();
  return (
    <Grid
      alertStore={alertStore}
      settingsStore={settingsStore}
      silenceFormStore={silenceFormStore}
    />
  );
};

const gridColorTitleBarStory = (): React.ReactNode => {
  const alertStore = makeGridAlertStore();
  const settingsStore = new Settings(null);
  settingsStore.alertGroupConfig.setDefaultCollapseState("expanded");
  const silenceFormStore = new SilenceFormStore();
  settingsStore.alertGroupConfig.setColorTitleBar(true);
  return (
    <Grid
      alertStore={alertStore}
      settingsStore={settingsStore}
      silenceFormStore={silenceFormStore}
    />
  );
};

const retainedSuppressionEvidenceStory = (): React.ReactNode => {
  const alertStore = makeAlertStore();
  const silenceFormStore = new SilenceFormStore();
  const alert = MockAlert(
    [
      {
        name: "summary",
        value: "Past alert retained by lifecycle history",
        visible: true,
        isLink: false,
        isAction: false,
      },
    ],
    [
      { name: "alertname", value: "APIHighErrorRate" },
      { name: "service", value: "payments" },
    ],
    "suppressed",
  );
  alert.isLive = false;
  alert.alertmanager = [];
  alert.suppressionCauses = [
    {
      target: "production",
      source: "alertmanager-a",
      silenceID: "maintenance-2025-08-14",
      resolutionStatus: "known",
      startsAt: "2018-08-14T16:00:00Z",
      endsAt: "2018-08-14T20:00:00Z",
      createdBy: "operator@example.com",
      comment: "Planned payment service maintenance",
      matchers: [
        {
          name: "service",
          value: "payments",
          isRegex: false,
          isEqual: true,
        },
      ],
    },
    {
      target: "production",
      source: "alertmanager-b",
      silenceID: "replication-lag-rule",
      resolutionStatus: "unresolved",
      startsAt: null,
      endsAt: null,
      createdBy: "",
      comment: "",
      matchers: [],
    },
  ];
  const group = MockAlertGroup(
    [{ name: "team", value: "payments" }],
    [alert],
    [],
    [],
    {},
  );

  return (
    <div className="p-3" style={{ maxWidth: "760px" }}>
      <ul className="list-group">
        <AlertCard
          group={group}
          alert={alert}
          showReceiver={true}
          showOnlyExpandedAnnotations={false}
          afterUpdate={() => {}}
          alertStore={alertStore}
          silenceFormStore={silenceFormStore}
          setIsMenuOpen={() => {}}
        />
      </ul>
    </div>
  );
};

const navBarStory = (): React.ReactNode => {
  const alertStore = new AlertStore([]);
  const settingsStore = new Settings(null);
  const silenceFormStore = new SilenceFormStore();
  alertStore.data.setUpstreams({
    counters: { total: 1, healthy: 1, failed: 0 },
    instances: [
      {
        name: "dev",
        cluster: "dev",
        clusterMembers: ["dev"],
        uri: "https://am.example.com",
        publicURI: "https://am.example.com",
        error: "",
        warnings: [],
        readonly: false,
        headers: {},
        corsCredentials: "include",
        version: "",
      },
    ],
    clusters: { dev: ["dev"] },
  });
  alertStore.info.setTotalAlerts(197);
  settingsStore.filterBarConfig.setAutohide(false);
  return (
    <NavBar
      alertStore={alertStore}
      settingsStore={settingsStore}
      silenceFormStore={silenceFormStore}
      fixedTop={false}
    />
  );
};

const navBarPausedStory = (): React.ReactNode => {
  const alertStore = new AlertStore([]);
  const settingsStore = new Settings(null);
  const silenceFormStore = new SilenceFormStore();
  alertStore.data.setUpstreams({
    counters: { total: 1, healthy: 1, failed: 0 },
    instances: [
      {
        name: "dev",
        cluster: "dev",
        clusterMembers: ["dev"],
        uri: "https://am.example.com",
        publicURI: "https://am.example.com",
        error: "",
        warnings: [],
        readonly: false,
        headers: {},
        corsCredentials: "include",
        version: "",
      },
    ],
    clusters: { dev: ["dev"] },
  });
  alertStore.info.setTotalAlerts(197);
  alertStore.status.pause();
  settingsStore.filterBarConfig.setAutohide(false);
  return (
    <NavBar
      alertStore={alertStore}
      settingsStore={settingsStore}
      silenceFormStore={silenceFormStore}
      fixedTop={false}
    />
  );
};

const fatalErrorStory = (): React.ReactNode => (
  <FatalError message="Something failed with a veryLongStringToTestTextWrappingveryLongStringToTestTextWrappingveryLongStringToTestTextWrapping" />
);

const upgradeNeededStory = (): React.ReactNode => (
  <UpgradeNeeded newVersion="1.2.3" reloadAfter={100000000} />
);

const reloadNeededStory = (): React.ReactNode => (
  <ReloadNeeded reloadAfter={100000000} />
);

const emptyGridStory = (): React.ReactNode => (
  <div className="text-center">
    <EmptyGrid />
  </div>
);

const noUpstreamStory = (): React.ReactNode => <NoUpstream />;

const internalErrorStory = (): React.ReactNode => (
  <InternalError
    message="React error boundary message with a veryLongStringToTestTextWrappingveryLongStringToTestTextWrappingveryLongStringToTestTextWrapping"
    secondsLeft={45}
    progressLeft={66}
  />
);

const mainModalStory = (): React.ReactNode => {
  const alertStore = makeAlertStore();
  const settingsStore = new Settings(null);
  alertStore.info.setVersion("1.2.3");
  return (
    <Modal>
      <MainModalContent
        alertStore={alertStore}
        settingsStore={settingsStore}
        onHide={() => {}}
        expandAllOptions={true}
      />
    </Modal>
  );
};

const overviewModalStory = (): React.ReactNode => {
  const alertStore = new AlertStore(["full"]);
  MockGrid(alertStore);
  return (
    <Modal>
      <OverviewModalContent alertStore={alertStore} onHide={() => {}} />
    </Modal>
  );
};

const managedSilenceStory = (): React.ReactNode => {
  const alertStore = makeAlertStore();
  const alertStoreReadOnly = makeReadOnlyAlertStore();
  const silenceFormStore = new SilenceFormStore();

  const silence = MockSilence();
  silence.startsAt = "2018-08-14T16:00:00Z";
  silence.endsAt = "2018-08-14T18:00:00Z";

  const expiredSilence = MockSilence();
  expiredSilence.startsAt = "2018-08-14T10:00:00Z";
  expiredSilence.endsAt = "2018-08-14T11:00:00Z";

  return (
    <div className="modal-dialog modal-lg" role="document">
      <div className="modal-content p-2" style={{ background: "transparent" }}>
        <ManagedSilence
          cluster="am"
          alertCount={123}
          alertCountAlwaysVisible={true}
          silence={silence}
          isExpired={false}
          alertStore={alertStore}
          silenceFormStore={silenceFormStore}
          onDidUpdate={() => {}}
        />
        <ManagedSilence
          cluster="am"
          alertCount={123}
          alertCountAlwaysVisible={true}
          silence={silence}
          isExpired={false}
          alertStore={alertStore}
          silenceFormStore={silenceFormStore}
          onDidUpdate={() => {}}
          isOpen={true}
        />
        <ManagedSilence
          cluster="ro"
          alertCount={123}
          alertCountAlwaysVisible={true}
          silence={silence}
          isExpired={false}
          alertStore={alertStoreReadOnly}
          silenceFormStore={silenceFormStore}
          onDidUpdate={() => {}}
          isOpen={true}
        />
        <ManagedSilence
          cluster="am"
          alertCount={123}
          alertCountAlwaysVisible={true}
          silence={expiredSilence}
          isExpired={true}
          alertStore={alertStore}
          silenceFormStore={silenceFormStore}
          onDidUpdate={() => {}}
        />
        <ManagedSilence
          cluster="am"
          alertCount={123}
          alertCountAlwaysVisible={true}
          silence={expiredSilence}
          isExpired={true}
          alertStore={alertStore}
          silenceFormStore={silenceFormStore}
          onDidUpdate={() => {}}
          isOpen={true}
        />
        <ManagedSilence
          cluster="ro"
          alertCount={123}
          alertCountAlwaysVisible={true}
          silence={expiredSilence}
          isExpired={true}
          alertStore={alertStoreReadOnly}
          silenceFormStore={silenceFormStore}
          onDidUpdate={() => {}}
          isOpen={true}
        />
      </div>
    </div>
  );
};

const silenceModalEditorReadOnlyStory = (): React.ReactNode => {
  const alertStore = makeReadOnlyAlertStore();
  const settingsStore = new Settings(null);
  const silenceFormStore = new SilenceFormStore();
  silenceFormStore.data.setAuthor("readonly@example.com");
  silenceFormStore.data.setComment("Read-only compact silence editor story");
  silenceFormStore.data.setMatchers([
    {
      id: "0",
      isRegex: false,
      isEqual: true,
      name: "alertname",
      values: [
        {
          value: "Host_Down",
          label: "Host_Down",
          wasCreated: false,
        },
      ],
    },
  ]);
  silenceFormStore.tab.setTab("editor");
  return (
    <Modal>
      <SilenceModalContent
        alertStore={alertStore}
        silenceFormStore={silenceFormStore}
        settingsStore={settingsStore}
        onHide={() => {}}
        onSubmitted={() => {}}
        registerCloseGuard={() => {}}
      />
    </Modal>
  );
};

const silenceModalEditorStory = (): React.ReactNode => {
  const alertStore = makeAlertStore();
  const settingsStore = new Settings(null);
  const silenceFormStore = new SilenceFormStore();

  silenceFormStore.data.setAuthor("me@example.com");
  silenceFormStore.data.setComment("This is a compact silence editor story");
  silenceFormStore.data.setMatchers([
    {
      id: "0",
      isRegex: true,
      isEqual: true,
      name: "service",
      values: [
        {
          value: "api|payments",
          label: "api|payments",
          wasCreated: false,
        },
      ],
    },
    {
      id: "1",
      isRegex: false,
      isEqual: true,
      name: "cluster",
      values: [
        {
          value: "prod",
          label: "prod",
          wasCreated: false,
        },
      ],
    },
  ]);
  silenceFormStore.data.setStart(new Date("2018-08-14T17:36:40Z"));
  silenceFormStore.data.setEnd(addDays(new Date("2018-08-14T19:00:00Z"), 5));
  silenceFormStore.tab.setTab("editor");

  return (
    <Modal>
      <SilenceModalContent
        alertStore={alertStore}
        silenceFormStore={silenceFormStore}
        settingsStore={settingsStore}
        onHide={() => {}}
        onSubmitted={() => {}}
        registerCloseGuard={() => {}}
      />
    </Modal>
  );
};

const silenceModalEditorValidationStory = (): React.ReactNode => {
  const alertStore = makeAlertStore();
  const settingsStore = new Settings(null);
  const silenceFormStore = new SilenceFormStore();

  silenceFormStore.data.setMatchers([
    {
      id: "0",
      isRegex: false,
      isEqual: true,
      name: "alertname",
      values: [
        {
          value: "Host_Down",
          label: "Host_Down",
          wasCreated: false,
        },
      ],
    },
  ]);
  silenceFormStore.data.setStart(new Date("2018-08-14T17:36:40Z"));
  silenceFormStore.data.setEnd(addDays(new Date("2018-08-14T19:00:00Z"), 5));
  silenceFormStore.tab.setTab("editor");

  return (
    <Modal>
      <SilenceModalContent
        alertStore={alertStore}
        silenceFormStore={silenceFormStore}
        settingsStore={settingsStore}
        onHide={() => {}}
        onSubmitted={() => {}}
        registerCloseGuard={() => {}}
      />
    </Modal>
  );
};

const toastStory = (): React.ReactNode => {
  const alertStore = new AlertStore([]);
  alertStore.info.setVersion("999.99.0");
  return (
    <div className="d-flex flex-column">
      <Toast
        icon={faExclamation}
        iconClass="text-danger"
        message={
          <ToastMessage
            title="Alertmanager am1 raised an error"
            message="connection refused"
          />
        }
        hasClose={true}
      />
      <Toast
        icon={faArrowUp}
        iconClass="text-success"
        message={<UpgradeToastMessage alertStore={alertStore} />}
        hasClose={false}
      />
    </div>
  );
};

const stories: StoryMap = {
  Grid: gridStory,
  GridColorTitleBar: gridColorTitleBarStory,
  RetainedSuppressionEvidence: retainedSuppressionEvidenceStory,
  NavBar: navBarStory,
  NavBarPaused: navBarPausedStory,
  FatalError: fatalErrorStory,
  UpgradeNeeded: upgradeNeededStory,
  ReloadNeeded: reloadNeededStory,
  EmptyGrid: emptyGridStory,
  NoUpstream: noUpstreamStory,
  InternalError: internalErrorStory,
  MainModal: mainModalStory,
  OverviewModal: overviewModalStory,
  ManagedSilence: managedSilenceStory,
  SilenceModalEditorReadOnly: silenceModalEditorReadOnlyStory,
  SilenceModalEditor: silenceModalEditorStory,
  SilenceModalEditorValidation: silenceModalEditorValidationStory,
  Toast: toastStory,
};

const lightTheme: ThemeCtx = {
  isDark: false,
  reactSelectStyles: ReactSelectStyles(ReactSelectColors.Light),
  animations: { duration: 0 },
};

const darkTheme: ThemeCtx = {
  isDark: true,
  reactSelectStyles: ReactSelectStyles(ReactSelectColors.Dark),
  animations: { duration: 0 },
};

const StoryRenderer = ({ storyFn }: { storyFn: () => React.ReactNode }) => (
  <div>
    <div className="theme-light" data-bs-theme="light">
      <ThemeContext.Provider value={lightTheme}>
        {storyFn()}
      </ThemeContext.Provider>
    </div>
    <div
      style={{
        height: "16px",
        width: "100%",
        backgroundColor: "#eee",
        marginTop: "4px",
        marginBottom: "4px",
      }}
    />
    <div className="theme-dark" data-bs-theme="dark">
      <ThemeContext.Provider value={darkTheme}>
        {storyFn()}
      </ThemeContext.Provider>
    </div>
  </div>
);

const VisualTestApp = () => {
  const storyName = window.location.hash.replace("#", "").split("?")[0] || "";
  const storyFn = stories[storyName];

  if (!storyFn) {
    return (
      <div style={{ padding: "2rem" }}>
        <h1>Visual Regression Test Stories</h1>
        <ul>
          {Object.keys(stories).map((name) => (
            <li key={name}>
              <a href={`#${name}`}>{name}</a>
            </li>
          ))}
        </ul>
      </div>
    );
  }

  return <StoryRenderer storyFn={storyFn} />;
};

const rootEl = document.getElementById("root");
if (rootEl) {
  const root = createRoot(rootEl);
  root.render(<VisualTestApp />);
}
