const projectInput = document.getElementById("projectId");
const navCollapseBtn = document.getElementById("navCollapseBtn");
const projectSelectorBtn = document.getElementById("projectSelectorBtn");
const globalSearchInput = document.getElementById("globalSearchInput");
const appbarSearchBtn = document.getElementById("appbarSearchBtn");
const appbarStarredBtn = document.getElementById("appbarStarredBtn");
const appbarThemeBtn = document.getElementById("appbarThemeBtn");
const appbarMoreBtn = document.getElementById("appbarMoreBtn");
const railIcons = Array.from(document.querySelectorAll(".rail-icon"));
const refreshBtn = document.getElementById("refreshBtn");
const loadProjectBtn = document.getElementById("loadProjectBtn");
const themeToggle = document.getElementById("themeToggle");
const createDatasetForm = document.getElementById("createDatasetForm");
const updateDatasetForm = document.getElementById("updateDatasetForm");
const createTableForm = document.getElementById("createTableForm");
const deleteDatasetBtn = document.getElementById("deleteDatasetBtn");
const updateTableMetaForm = document.getElementById("updateTableMetaForm");
const saveQueryForm = document.getElementById("saveQueryForm");
const savedQueryName = document.getElementById("savedQueryName");
const runQueryForm = document.getElementById("runQueryForm");
const queryText = document.getElementById("queryText");
const mainTabs = document.getElementById("mainTabs");
const queryRunStatus = document.getElementById("queryRunStatus");
const queryResultsMeta = document.getElementById("queryResultsMeta");
const queryResultsTable = document.getElementById("queryResultsTable");
const queryResultsJson = document.getElementById("queryResultsJson");
const queryResultsStats = document.getElementById("queryResultsStats");
const resultTabs = document.getElementById("resultTabs");
const tableDetailsTabs = document.getElementById("tableDetailsTabs");
const jobsHistoryTabs = document.getElementById("jobsHistoryTabs");
const jobsHistoryHint = document.getElementById("jobsHistoryHint");
const refreshJobBtn = document.getElementById("refreshJobBtn");
const cancelJobBtn = document.getElementById("cancelJobBtn");
const selectedJobHint = document.getElementById("selectedJobHint");
const jobDetailsJson = document.getElementById("jobDetailsJson");
const jobsFilterForm = document.getElementById("jobsFilterForm");
const jobsStateFilter = document.getElementById("jobsStateFilter");
const jobsUserEmailFilter = document.getElementById("jobsUserEmailFilter");
const allUsersToggle = document.getElementById("allUsersToggle");
const clearJobsFiltersBtn = document.getElementById("clearJobsFiltersBtn");
const jobsPrevBtn = document.getElementById("jobsPrevBtn");
const jobsNextBtn = document.getElementById("jobsNextBtn");
const jobsPageHint = document.getElementById("jobsPageHint");

const healthStatus = document.getElementById("healthStatus");
const capabilitiesStatus = document.getElementById("capabilitiesStatus");
const jobsStatus = document.getElementById("jobsStatus");
const emulatorTarget = document.getElementById("emulatorTarget");
const explorerTree = document.getElementById("explorerTree");
const explorerSearchInput = document.getElementById("explorerSearchInput");
const clearExplorerSearchBtn = document.getElementById("clearExplorerSearchBtn");
const explorerCapabilityFilter = document.getElementById("explorerCapabilityFilter");
const datasetMetaDatasetId = document.getElementById("datasetMetaDatasetId");
const datasetFriendlyNameInput = document.getElementById("datasetFriendlyNameInput");
const datasetLocationInput = document.getElementById("datasetLocationInput");
const datasetLabelsInput = document.getElementById("datasetLabelsInput");
const datasetSummaryStatus = document.getElementById("datasetSummaryStatus");
const datasetSummaryCapabilityNote = document.getElementById("datasetSummaryCapabilityNote");
const datasetSummaryId = document.getElementById("datasetSummaryId");
const datasetSummaryFriendlyName = document.getElementById("datasetSummaryFriendlyName");
const datasetSummaryLocation = document.getElementById("datasetSummaryLocation");
const datasetSummaryTables = document.getElementById("datasetSummaryTables");
const datasetSummaryActionStatus = document.getElementById("datasetSummaryActionStatus");
const datasetQueryBtn = document.getElementById("datasetQueryBtn");
const datasetListTablesBtn = document.getElementById("datasetListTablesBtn");
const datasetCopyIdBtn = document.getElementById("datasetCopyIdBtn");
const datasetSummaryLabels = document.getElementById("datasetSummaryLabels");
const breadcrumbDatasetChip = document.getElementById("breadcrumbDatasetChip");
const breadcrumbTableChip = document.getElementById("breadcrumbTableChip");
const tableDetailsMeta = document.getElementById("tableDetailsMeta");
const tableInfoName = document.getElementById("tableInfoName");
const tableInfoDescription = document.getElementById("tableInfoDescription");
const tableInfoETag = document.getElementById("tableInfoETag");
const tableInfoCreated = document.getElementById("tableInfoCreated");
const tableInfoUpdated = document.getElementById("tableInfoUpdated");
const tableInfoLabels = document.getElementById("tableInfoLabels");
const tableFriendlyNameInput = document.getElementById("tableFriendlyNameInput");
const tableDescriptionInput = document.getElementById("tableDescriptionInput");
const tableLabelsInput = document.getElementById("tableLabelsInput");
const updateTableLabelsBtn = document.getElementById("updateTableLabelsBtn");
const tableSchemaList = document.getElementById("tableSchemaList");
const tablePreviewMeta = document.getElementById("tablePreviewMeta");
const tablePreviewTable = document.getElementById("tablePreviewTable");
const tableDetailsJson = document.getElementById("tableDetailsJson");
const queryTableBtn = document.getElementById("queryTableBtn");
const copyTableBtn = document.getElementById("copyTableBtn");
const deleteTableBtn = document.getElementById("deleteTableBtn");
const tableActionStatus = document.getElementById("tableActionStatus");
const jobsList = document.getElementById("jobsList");
const capabilitiesJson = document.getElementById("capabilitiesJson");
const savedQueriesList = document.getElementById("savedQueriesList");
const exportSavedQueriesBtn = document.getElementById("exportSavedQueriesBtn");
const importSavedQueriesBtn = document.getElementById("importSavedQueriesBtn");
const savedQueriesImportInput = document.getElementById("savedQueriesImportInput");

let selectedJobId = "";
let selectedDatasetId = "";
let selectedTableId = "";
let jobsPageToken = "";
let jobsNextPageToken = "";
let jobsPageHistory = [];
let lastJobsCount = 0;
let explorerFilterText = "";
let explorerCapabilityFilterText = "all";
let explorerDatasetsCache = [];
let explorerTablesCache = new Map();
let explorerCollapsedDatasets = new Set();
let capabilityCounts = { supported: 0, partial: 0, unsupported: 0 };
let capabilityStatusByKey = new Map();

const capabilityFilterOptionLabels = {
  all: "Capabilities: All",
  supported: "Only SUPPORTED",
  partial: "Only PARTIAL",
  unsupported: "Only UNSUPPORTED",
};

const savedQueriesStorageKey = "locaql.savedQueries";
const themeStorageKey = "locaql.theme";
const explorerCapabilityFilterStorageKey = "locaql.explorer.capabilityFilter";

async function fetchJson(path, options) {
  const res = await fetch(path, options);
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`${res.status} ${res.statusText}: ${body}`);
  }
  if (res.status === 204) {
    return null;
  }
  const body = await res.text();
  if (!body) {
    return null;
  }
  return JSON.parse(body);
}

function renderList(target, items, formatter) {
  target.innerHTML = "";
  if (!items.length) {
    const li = document.createElement("li");
    li.textContent = "No data.";
    target.appendChild(li);
    return;
  }
  for (const item of items) {
    const li = document.createElement("li");
    li.textContent = formatter(item);
    target.appendChild(li);
  }
}

function getProjectId() {
  return projectInput.value.trim() || "p1";
}

function formatEpochMillis(value) {
  const ms = Number(value || 0);
  if (!Number.isFinite(ms) || ms <= 0) {
    return "-";
  }
  return new Date(ms).toLocaleString();
}

function updateProjectChip() {
  if (projectSelectorBtn) {
    projectSelectorBtn.textContent = getProjectId();
  }
}

function syncCreateTableDatasetInput() {
  const datasetInput = document.getElementById("newTableDatasetId");
  if (!datasetInput) {
    return;
  }
  datasetInput.value = selectedDatasetId || datasetInput.value || "analytics";
}

function syncDatasetMetaInputs() {
  if (!datasetMetaDatasetId) {
    return;
  }
  const activeDatasetId = selectedDatasetId || datasetMetaDatasetId.value || "analytics";
  datasetMetaDatasetId.value = activeDatasetId;
  const match = explorerDatasetsCache.find((ds) => (ds.datasetReference || {}).datasetId === activeDatasetId);
  if (datasetFriendlyNameInput) {
    datasetFriendlyNameInput.value = (match && match.friendlyName) || "";
  }
  if (datasetLocationInput) {
    datasetLocationInput.value = (match && match.location) || "";
  }
  if (datasetLabelsInput) {
    datasetLabelsInput.value = match && match.labels ? JSON.stringify(match.labels) : "";
  }
  syncDatasetSummary(activeDatasetId, match);
}

function syncDatasetSummary(activeDatasetId, datasetMeta) {
  const datasetId = activeDatasetId || "";
  const tables = datasetId ? (explorerTablesCache.get(datasetId) || []) : [];
  if (datasetSummaryStatus) {
    datasetSummaryStatus.textContent = datasetId ? "selected" : "none";
  }
  if (datasetSummaryId) {
    datasetSummaryId.textContent = datasetId || "-";
  }
  if (datasetSummaryFriendlyName) {
    datasetSummaryFriendlyName.textContent = (datasetMeta && datasetMeta.friendlyName) || "-";
  }
  if (datasetSummaryLocation) {
    datasetSummaryLocation.textContent = (datasetMeta && datasetMeta.location) || "-";
  }
  if (datasetSummaryTables) {
    datasetSummaryTables.textContent = String(tables.length || 0);
  }
  if (datasetSummaryLabels) {
    datasetSummaryLabels.textContent = JSON.stringify((datasetMeta && datasetMeta.labels) || {}, null, 2);
  }
  if (datasetSummaryCapabilityNote) {
    datasetSummaryCapabilityNote.textContent = `Capability signal: ${capabilityCounts.supported} supported / ${capabilityCounts.partial} partial / ${capabilityCounts.unsupported} unsupported`;
  }
  if (datasetSummaryActionStatus) {
    datasetSummaryActionStatus.textContent = datasetId ? "actions ready" : "actions disabled";
  }
  if (datasetQueryBtn) datasetQueryBtn.disabled = !datasetId;
  if (datasetListTablesBtn) datasetListTablesBtn.disabled = !datasetId;
  if (datasetCopyIdBtn) datasetCopyIdBtn.disabled = !datasetId;
}

function combineCapabilityStatus(keys) {
  const statuses = keys
    .map((k) => {
      const entry = capabilityStatusByKey.get(k);
      return entry && entry.status ? String(entry.status).toLowerCase() : "";
    })
    .filter(Boolean);

  if (!statuses.length) {
    return "partial";
  }
  if (statuses.includes("unsupported")) {
    return "unsupported";
  }
  if (statuses.includes("partial")) {
    return "partial";
  }
  if (statuses.every((s) => s === "supported")) {
    return "supported";
  }
  return "partial";
}

function matchesCapabilityFilter(status) {
  return explorerCapabilityFilterText === "all" || status === explorerCapabilityFilterText;
}

function updateExplorerCapabilityFilterOptions() {
  if (!explorerCapabilityFilter) {
    return;
  }

  // Until capabilities are loaded, keep static labels to avoid misleading counts.
  if (capabilityStatusByKey.size === 0) {
    for (const option of explorerCapabilityFilter.options) {
      const base = capabilityFilterOptionLabels[option.value] || option.textContent;
      option.textContent = base;
    }
    return;
  }

  const searchTerm = explorerFilterText.trim().toLowerCase();
  const projectId = getProjectId();
  const projectMatches = searchTerm ? matchExplorerFilter(projectId, searchTerm) : true;
  const counts = { supported: 0, partial: 0, unsupported: 0 };
  let contextCount = 0;

  for (const ds of explorerDatasetsCache) {
    const dsRef = ds.datasetReference || {};
    const datasetId = dsRef.datasetId || "";
    const tables = explorerTablesCache.get(datasetId) || [];
    const datasetMatches = searchTerm ? matchExplorerFilter(datasetId, searchTerm) : true;

    let filteredTables = tables;
    if (searchTerm && !projectMatches && !datasetMatches) {
      filteredTables = tables.filter((t) => {
        const tRef = t.tableReference || {};
        const tableId = tRef.tableId || "";
        return matchExplorerFilter(tableId, searchTerm);
      });
    }

    const routineSearchMatch = !searchTerm || matchExplorerFilter("routines", searchTerm);
    const modelSearchMatch = !searchTerm || matchExplorerFilter("models", searchTerm);
    const hasVisibleChildren = filteredTables.length > 0 || routineSearchMatch || modelSearchMatch;
    const visibleBySearch = !searchTerm || projectMatches || datasetMatches || hasVisibleChildren;
    if (!visibleBySearch) {
      continue;
    }

    const isDatasetDirectMatch = !searchTerm || projectMatches || datasetMatches;

    const datasetStatus = combineCapabilityStatus([
      "rest.datasets.get",
      "rest.datasets.patch",
      "console.ui.resource_forms.basic",
    ]);
    if (isDatasetDirectMatch) {
      counts[datasetStatus] = (counts[datasetStatus] || 0) + 1;
    } else if (hasVisibleChildren) {
      contextCount += 1;
    }

    const tableStatus = combineCapabilityStatus([
      "rest.tables.get",
      "rest.tabledata.list.pagination",
      "console.ui.table_details.preview_schema_metadata",
    ]);
    counts[tableStatus] = (counts[tableStatus] || 0) + filteredTables.length;

    if (routineSearchMatch) {
      counts.unsupported += 1;
    }
    if (modelSearchMatch) {
      counts.unsupported += 1;
    }
  }

  for (const option of explorerCapabilityFilter.options) {
    const value = option.value;
    if (value === "all") {
      const total = counts.supported + counts.partial + counts.unsupported;
      option.textContent = `${capabilityFilterOptionLabels.all} (${total}) · CONTEXT (${contextCount})`;
      continue;
    }
    const base = capabilityFilterOptionLabels[value] || option.textContent;
    option.textContent = `${base} (${counts[value] || 0})`;
  }
}

function statusBadgeLabel(status) {
  if (status === "supported") return "SUPPORTED";
  if (status === "unsupported") return "UNSUPPORTED";
  if (status === "context") return "CONTEXT";
  return "PARTIAL";
}

function buildCapabilityBadge(status, titleText) {
  const badge = document.createElement("span");
  badge.className = `cap-badge cap-${status}`;
  badge.textContent = statusBadgeLabel(status);
  if (titleText) {
    badge.title = titleText;
  }
  return badge;
}

async function copyDatasetId() {
  if (!selectedDatasetId) return;
  if (datasetSummaryActionStatus) datasetSummaryActionStatus.textContent = "copying dataset id";
  try {
    await navigator.clipboard.writeText(selectedDatasetId);
    if (datasetSummaryActionStatus) datasetSummaryActionStatus.textContent = "dataset id copied";
  } catch (_) {
    if (datasetSummaryActionStatus) datasetSummaryActionStatus.textContent = "copy unavailable in this browser";
  }
}

function listSelectedDatasetTables() {
  if (!selectedDatasetId) return;
  const projectId = getProjectId();
  queryText.value = `SELECT table_name\nFROM \`${projectId}.${selectedDatasetId}.INFORMATION_SCHEMA.TABLES\`\nORDER BY table_name\nLIMIT 200;`;
  setActiveMainTab("query-workspace");
  if (datasetSummaryActionStatus) datasetSummaryActionStatus.textContent = "table listing query drafted";
}

function querySelectedDataset() {
  if (!selectedDatasetId) return;
  const projectId = getProjectId();
  queryText.value = `SELECT *\nFROM \`${projectId}.${selectedDatasetId}.__TABLES_SUMMARY__\`\nLIMIT 100;`;
  setActiveMainTab("query-workspace");
  if (datasetSummaryActionStatus) datasetSummaryActionStatus.textContent = "dataset query drafted";
}

async function selectDataset(projectId, datasetId) {
  selectedDatasetId = datasetId;
  selectedTableId = "";
  syncCreateTableDatasetInput();
  syncDatasetMetaInputs();
  if (breadcrumbDatasetChip) {
    breadcrumbDatasetChip.textContent = datasetId || projectId;
  }
  if (breadcrumbTableChip) {
    breadcrumbTableChip.textContent = "Table";
  }
  await renderExplorerTree(projectId);
  updateTableActionState();
}

function hasSelectedTable() {
  return Boolean(selectedDatasetId && selectedTableId);
}

function updateTableActionState(statusText) {
  const enabled = hasSelectedTable();
  if (queryTableBtn) queryTableBtn.disabled = !enabled;
  if (copyTableBtn) copyTableBtn.disabled = !enabled;
  if (deleteTableBtn) deleteTableBtn.disabled = !enabled;
  if (tableActionStatus) {
    if (statusText) {
      tableActionStatus.textContent = statusText;
    } else {
      tableActionStatus.textContent = enabled
        ? `${selectedDatasetId}.${selectedTableId} selected`
        : "select a table to enable actions";
    }
  }
}

async function selectTable(projectId, datasetId, tableId) {
  selectedDatasetId = datasetId;
  selectedTableId = tableId;
  syncCreateTableDatasetInput();
  syncDatasetMetaInputs();
  if (breadcrumbDatasetChip) {
    breadcrumbDatasetChip.textContent = datasetId;
  }
  if (breadcrumbTableChip) {
    breadcrumbTableChip.textContent = tableId || "Table";
  }
  await renderExplorerTree(projectId);
  await Promise.all([
    loadTablePreview(projectId, datasetId, tableId),
    loadTableDetails(projectId, datasetId, tableId),
  ]);
  updateTableActionState();
}

function inferValueType(value) {
  const raw = String(value ?? "").trim();
  if (raw === "" || raw.toLowerCase() === "null") return "STRING";
  if (/^(true|false)$/i.test(raw)) return "BOOL";
  if (/^-?\d+$/.test(raw)) return "INT64";
  if (/^-?\d+\.\d+$/.test(raw)) return "FLOAT64";
  if (!Number.isNaN(Date.parse(raw)) && /\d{4}-\d{2}-\d{2}/.test(raw)) return "TIMESTAMP";
  return "STRING";
}

function inferColumnType(values) {
  const types = new Set(values.map(inferValueType));
  if (types.size === 1) {
    return Array.from(types)[0];
  }
  if (types.has("STRING")) return "STRING";
  if (types.has("FLOAT64") && types.has("INT64")) return "FLOAT64";
  return "STRING";
}

function normalizeSavedQuery(item) {
  if (!item || typeof item !== "object") {
    return null;
  }
  const name = String(item.name || "").trim();
  if (!name) {
    return null;
  }

  let versions = [];
  if (Array.isArray(item.versions) && item.versions.length > 0) {
    versions = item.versions
      .map((v) => ({
        sql: String(v?.sql || "").trim(),
        savedAt: Number(v?.savedAt) || Date.now(),
      }))
      .filter((v) => v.sql !== "");
  } else {
    const legacySQL = String(item.sql || "").trim();
    if (legacySQL) {
      versions = [{ sql: legacySQL, savedAt: Number(item.savedAt) || Date.now() }];
    }
  }

  if (!versions.length) {
    return null;
  }

  let currentVersion = Number(item.currentVersion);
  if (!Number.isInteger(currentVersion) || currentVersion < 0 || currentVersion >= versions.length) {
    currentVersion = versions.length - 1;
  }

  return { name, versions, currentVersion };
}

function getSavedQueries() {
  try {
    const raw = localStorage.getItem(savedQueriesStorageKey);
    if (!raw) {
      return [];
    }
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) {
      return [];
    }
    return parsed
      .map(normalizeSavedQuery)
      .filter((item) => item !== null)
      .slice(0, 50);
  } catch (_) {
    return [];
  }
}

function setSavedQueries(items) {
  const normalized = (items || [])
    .map(normalizeSavedQuery)
    .filter((item) => item !== null)
    .slice(0, 50);
  localStorage.setItem(savedQueriesStorageKey, JSON.stringify(normalized));
}

function upsertSavedQuery(name, sql) {
  const safeName = String(name || "").trim();
  const safeSQL = String(sql || "").trim();
  if (!safeName || !safeSQL) {
    return false;
  }

  const items = getSavedQueries();
  const existing = items.find((q) => q.name === safeName);
  if (!existing) {
    items.unshift({
      name: safeName,
      versions: [{ sql: safeSQL, savedAt: Date.now() }],
      currentVersion: 0,
    });
    setSavedQueries(items);
    return true;
  }

  const current = existing.versions[existing.currentVersion] || existing.versions[existing.versions.length - 1];
  if (current && current.sql === safeSQL) {
    return false;
  }

  existing.versions.push({ sql: safeSQL, savedAt: Date.now() });
  if (existing.versions.length > 20) {
    existing.versions = existing.versions.slice(existing.versions.length - 20);
  }
  existing.currentVersion = existing.versions.length - 1;
  setSavedQueries(items);
  return true;
}

function mergeSavedQueries(importedItems) {
  const current = getSavedQueries();
  const byName = new Map(current.map((item) => [item.name, item]));

  for (const raw of importedItems) {
    const incoming = normalizeSavedQuery(raw);
    if (!incoming) {
      continue;
    }

    const existing = byName.get(incoming.name);
    if (!existing) {
      byName.set(incoming.name, incoming);
      continue;
    }

    const mergedVersions = [...existing.versions];
    for (const version of incoming.versions) {
      const duplicated = mergedVersions.some((v) => v.sql === version.sql && v.savedAt === version.savedAt);
      if (!duplicated) {
        mergedVersions.push(version);
      }
    }
    mergedVersions.sort((a, b) => a.savedAt - b.savedAt);
    existing.versions = mergedVersions.slice(-20);
    existing.currentVersion = existing.versions.length - 1;
  }

  const merged = Array.from(byName.values()).slice(0, 50);
  setSavedQueries(merged);
}

async function importSavedQueriesFromFile(file) {
  const text = await file.text();
  const parsed = JSON.parse(text);
  if (!Array.isArray(parsed)) {
    throw new Error("Invalid JSON format: expected an array");
  }
  mergeSavedQueries(parsed);
}

function exportSavedQueries() {
  const items = getSavedQueries();
  const blob = new Blob([JSON.stringify(items, null, 2)], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `locaql-saved-queries-${new Date().toISOString().slice(0, 10)}.json`;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

async function copyQueryShareLink(sql) {
  const url = new URL(window.location.href);
  url.searchParams.set("query", sql);
  if (navigator.clipboard && navigator.clipboard.writeText) {
    await navigator.clipboard.writeText(url.toString());
  } else {
    window.prompt("Copy query URL:", url.toString());
  }
}

function loadQueryFromURL() {
  const url = new URL(window.location.href);
  const queryParam = url.searchParams.get("query");
  if (!queryParam) {
    return;
  }
  queryText.value = queryParam;
  queryRunStatus.textContent = "query loaded from URL";
}

function applyTheme(theme) {
  document.body.setAttribute("data-theme", theme);
  themeToggle.textContent = theme === "dark" ? "Light" : "Dark";
  localStorage.setItem(themeStorageKey, theme);
}

function currentTheme() {
  return document.body.getAttribute("data-theme") || "light";
}

function updateSelectedJobHint() {
  selectedJobHint.textContent = selectedJobId ? `selected: ${selectedJobId}` : "selected: none";
}

async function loadHealth() {
  const health = await fetchJson("/api/_emulator/health");
  healthStatus.textContent = health.status || "unknown";
  healthStatus.className = health.status === "ok" ? "metric status-ok" : "metric status-warn";
}

async function loadCapabilities() {
  const caps = await fetchJson("/api/_emulator/capabilities");
  const entries = Object.entries(caps.capabilities || {});
  capabilityStatusByKey = new Map(entries);
  const supported = entries.filter(([, v]) => v.status === "supported").length;
  const partial = entries.filter(([, v]) => v.status === "partial").length;
  const unsupported = entries.filter(([, v]) => v.status === "unsupported").length;
  capabilityCounts = { supported, partial, unsupported };
  capabilitiesStatus.textContent = `${supported} supported / ${partial} partial`;
  capabilitiesJson.textContent = JSON.stringify(caps, null, 2);
  await renderExplorerTree(getProjectId());
  syncDatasetSummary(selectedDatasetId, explorerDatasetsCache.find((ds) => (ds.datasetReference || {}).datasetId === selectedDatasetId));
}

async function loadDatasets(projectId) {
  const data = await fetchJson(`/api/bigquery/v2/projects/${encodeURIComponent(projectId)}/datasets?maxResults=50`);
  explorerDatasetsCache = data.datasets || [];
  explorerTablesCache = new Map();

  await Promise.all(explorerDatasetsCache.map(async (ds) => {
    const dsRef = ds.datasetReference || {};
    const datasetId = dsRef.datasetId || "";
    if (!datasetId) {
      return;
    }
    try {
      const tablesResp = await fetchJson(`/api/bigquery/v2/projects/${encodeURIComponent(projectId)}/datasets/${encodeURIComponent(datasetId)}/tables?maxResults=50`);
      explorerTablesCache.set(datasetId, tablesResp.tables || []);
    } catch (_) {
      explorerTablesCache.set(datasetId, []);
    }
  }));

  await renderExplorerTree(projectId);
}

function matchExplorerFilter(value, term) {
  return String(value || "").toLowerCase().includes(term);
}

async function renderExplorerTree(projectId) {
  explorerTree.innerHTML = "";
  updateExplorerCapabilityFilterOptions();
  const searchTerm = explorerFilterText.trim().toLowerCase();
  const projectMatches = searchTerm ? matchExplorerFilter(projectId, searchTerm) : true;
  const allTablesCount = Array.from(explorerTablesCache.values()).reduce((sum, tables) => sum + tables.length, 0);

  const projectNode = document.createElement("div");
  projectNode.className = "node project";
  projectNode.textContent = `Project: ${projectId} • ${explorerDatasetsCache.length} datasets • ${allTablesCount} tables`;
  explorerTree.appendChild(projectNode);

  let visibleNodes = 0;

  for (const ds of explorerDatasetsCache) {
    const dsRef = ds.datasetReference || {};
    const datasetId = dsRef.datasetId || "";
    const tables = explorerTablesCache.get(datasetId) || [];
    const datasetMatches = searchTerm ? matchExplorerFilter(datasetId, searchTerm) : true;
    const datasetStatus = combineCapabilityStatus([
      "rest.datasets.get",
      "rest.datasets.patch",
      "console.ui.resource_forms.basic",
    ]);

    let filteredTables = tables;
    if (searchTerm && !projectMatches && !datasetMatches) {
      filteredTables = tables.filter((t) => {
        const tRef = t.tableReference || {};
        const tableId = tRef.tableId || "";
        return matchExplorerFilter(tableId, searchTerm);
      });
    }

    const filteredTableEntries = filteredTables
      .map((t) => {
        const tRef = t.tableReference || {};
        const tableId = tRef.tableId || "";
        const status = combineCapabilityStatus([
          "rest.tables.get",
          "rest.tabledata.list.pagination",
          "console.ui.table_details.preview_schema_metadata",
        ]);
        return { table: t, tableId, status };
      })
      .filter((entry) => matchesCapabilityFilter(entry.status));

    const routineSearchMatch = !searchTerm || matchExplorerFilter("routines", searchTerm);
    const modelSearchMatch = !searchTerm || matchExplorerFilter("models", searchTerm);
    const routineVisible = routineSearchMatch && matchesCapabilityFilter("unsupported");
    const modelVisible = modelSearchMatch && matchesCapabilityFilter("unsupported");

    const hasVisibleChildren = filteredTableEntries.length > 0 || routineVisible || modelVisible;
    const visibleBySearch = !searchTerm || projectMatches || datasetMatches || hasVisibleChildren;
    const visibleByCapability = matchesCapabilityFilter(datasetStatus) || hasVisibleChildren;
    if (!visibleBySearch || !visibleByCapability) {
      continue;
    }

    const datasetSection = document.createElement("div");
    datasetSection.className = "dataset-section";

    const datasetHeader = document.createElement("button");
    datasetHeader.type = "button";
    datasetHeader.className = "node dataset dataset-header";
    datasetHeader.setAttribute("aria-expanded", explorerCollapsedDatasets.has(datasetId) ? "false" : "true");

    const datasetTitle = document.createElement("span");
    datasetTitle.className = "dataset-title";
    datasetTitle.textContent = `Dataset: ${datasetId}`;

    const datasetBadgeStatus = matchesCapabilityFilter(datasetStatus) || explorerCapabilityFilterText === "all"
      ? datasetStatus
      : "context";
    const datasetBadgeTitle = datasetBadgeStatus === "context"
      ? "Dataset shown as parent container for matching child resources"
      : "Dataset capabilities";
    datasetTitle.appendChild(buildCapabilityBadge(datasetBadgeStatus, datasetBadgeTitle));

    const datasetMeta = document.createElement("span");
    datasetMeta.className = "dataset-meta";
    datasetMeta.textContent = `${filteredTableEntries.length}/${tables.length} tables`;

    datasetHeader.appendChild(datasetTitle);
    datasetHeader.appendChild(datasetMeta);

    if (datasetId === selectedDatasetId && !selectedTableId) {
      datasetHeader.classList.add("active");
    }

    datasetHeader.addEventListener("click", async () => {
      await selectDataset(projectId, datasetId);
      if (explorerCollapsedDatasets.has(datasetId)) {
        explorerCollapsedDatasets.delete(datasetId);
      } else {
        explorerCollapsedDatasets.add(datasetId);
      }
      await renderExplorerTree(projectId);
    });

    datasetSection.appendChild(datasetHeader);
    visibleNodes++;

    if (!datasetId) {
      continue;
    }

    if (datasetId === selectedDatasetId && breadcrumbDatasetChip) {
      breadcrumbDatasetChip.textContent = datasetId;
    }

    const tableGroup = document.createElement("div");
    tableGroup.className = "dataset-tables";
    if (explorerCollapsedDatasets.has(datasetId)) {
      tableGroup.classList.add("collapsed");
    }

    const datasetShouldAutoOpen = searchTerm && hasVisibleChildren;
    if (datasetShouldAutoOpen) {
      explorerCollapsedDatasets.delete(datasetId);
      tableGroup.classList.remove("collapsed");
      datasetHeader.setAttribute("aria-expanded", "true");
    }

    for (const entry of filteredTableEntries) {
      const t = entry.table;
      const tableId = entry.tableId;
      const tableNode = document.createElement("div");
      tableNode.className = "node table";
      if (datasetId === selectedDatasetId && tableId === selectedTableId) {
        tableNode.classList.add("active");
      }
      const tableName = document.createElement("span");
      tableName.className = "node-label";
      tableName.textContent = tableId;
      tableNode.appendChild(tableName);

      tableNode.appendChild(buildCapabilityBadge(entry.status, "Table capabilities"));

      tableNode.addEventListener("click", async () => {
        await selectTable(projectId, datasetId, tableId);
      });

      tableGroup.appendChild(tableNode);
      visibleNodes++;
    }

    // Routines placeholder
    if (routineVisible) {
      const routineNode = document.createElement("div");
      routineNode.className = "node table meta-text";
      routineNode.style.fontStyle = "italic";
      const routineLabel = document.createElement("span");
      routineLabel.className = "node-label";
      routineLabel.textContent = "Routines";
      routineNode.appendChild(routineLabel);
      routineNode.appendChild(buildCapabilityBadge("unsupported", "Backend routine resources are not implemented"));
      tableGroup.appendChild(routineNode);
    }

    // Models placeholder
    if (modelVisible) {
      const modelNode = document.createElement("div");
      modelNode.className = "node table meta-text";
      modelNode.style.fontStyle = "italic";
      const modelLabel = document.createElement("span");
      modelLabel.className = "node-label";
      modelLabel.textContent = "Models";
      modelNode.appendChild(modelLabel);
      modelNode.appendChild(buildCapabilityBadge("unsupported", "Backend model resources are not implemented"));
      tableGroup.appendChild(modelNode);
    }

    datasetSection.appendChild(tableGroup);
    explorerTree.appendChild(datasetSection);
  }

  if (visibleNodes === 0) {
    const emptyNode = document.createElement("div");
    emptyNode.className = "node dataset";
    emptyNode.textContent = "No resources match your search.";
    explorerTree.appendChild(emptyNode);
  }
}

function renderTableData(target, columns, rows) {
  target.innerHTML = "";
  if (!columns.length) {
    columns = ["result"];
  }

  const thead = document.createElement("thead");
  const headRow = document.createElement("tr");
  for (const col of columns) {
    const th = document.createElement("th");
    th.textContent = col;
    headRow.appendChild(th);
  }
  thead.appendChild(headRow);

  const tbody = document.createElement("tbody");
  if (!rows.length) {
    const tr = document.createElement("tr");
    const td = document.createElement("td");
    td.colSpan = columns.length;
    td.textContent = "No rows";
    tr.appendChild(td);
    tbody.appendChild(tr);
  } else {
    for (const row of rows) {
      const tr = document.createElement("tr");
      for (const value of row) {
        const td = document.createElement("td");
        td.textContent = value;
        tr.appendChild(td);
      }
      tbody.appendChild(tr);
    }
  }

  target.appendChild(thead);
  target.appendChild(tbody);
}

async function loadTableDetails(projectId, datasetId, tableId) {
  try {
    const [tableMeta, preview] = await Promise.all([
      fetchJson(`/api/bigquery/v2/projects/${encodeURIComponent(projectId)}/datasets/${encodeURIComponent(datasetId)}/tables/${encodeURIComponent(tableId)}`),
      fetchJson(`/api/bigquery/v2/projects/${encodeURIComponent(projectId)}/tabledata/${encodeURIComponent(datasetId)}/${encodeURIComponent(tableId)}/data?maxResults=15`),
    ]);

    tableDetailsMeta.textContent = `${projectId}:${datasetId}.${tableId}`;
    tableInfoName.textContent = tableMeta.id || `${projectId}:${datasetId}.${tableId}`;
    tableInfoDescription.textContent = tableMeta.description || "No description";
    tableInfoETag.textContent = tableMeta.etag || "-";
    tableInfoCreated.textContent = formatEpochMillis(tableMeta.creationTime);
    tableInfoUpdated.textContent = formatEpochMillis(tableMeta.lastModifiedTime);
    tableInfoLabels.textContent = JSON.stringify(tableMeta.labels || {}, null, 2);
    tableDetailsJson.textContent = JSON.stringify(tableMeta, null, 2);
    if (tableFriendlyNameInput) {
      tableFriendlyNameInput.value = tableMeta.friendlyName || "";
    }
    if (tableDescriptionInput) {
      tableDescriptionInput.value = tableMeta.description || "";
    }
    if (tableLabelsInput) {
      tableLabelsInput.value = tableMeta.labels ? JSON.stringify(tableMeta.labels) : "";
    }

    const schemaFields = (((tableMeta.schema || {}).fields) || []).map((f) => ({
      name: f.name || "field",
      type: f.type || "STRING",
      mode: f.mode || "NULLABLE",
      description: f.description || "",
    }));

    const rows = (preview.rows || []).map((r) => (r.f || []).map((cell) => String(cell.v ?? "")));
    const inferredColumns = rows.length > 0 ? rows[0].map((_, idx) => `col_${idx + 1}`) : [];
    const schemaColumns = schemaFields.map((f) => f.name);
    const columns = schemaColumns.length ? schemaColumns : inferredColumns;

    tableSchemaList.innerHTML = "";
    if (!columns.length) {
      const li = document.createElement("li");
      li.className = "schema-item";
      li.textContent = "No schema available from emulator for this table.";
      tableSchemaList.appendChild(li);
    } else {
      const resolvedSchema = schemaFields.length
        ? schemaFields
        : columns.map((name, idx) => {
          const values = rows.map((r) => r[idx]);
          return { name, type: inferColumnType(values), mode: "NULLABLE", description: "Inferred from preview" };
        });

      for (const field of resolvedSchema) {
        const li = document.createElement("li");
        li.className = "schema-item";

        const head = document.createElement("div");
        head.className = "schema-item-head";

        const name = document.createElement("span");
        name.className = "schema-name";
        name.textContent = field.name;

        const type = document.createElement("span");
        type.className = "schema-type";
        type.textContent = `${field.type} · ${field.mode}`;

        head.appendChild(name);
        head.appendChild(type);
        li.appendChild(head);

        if (field.description) {
          const desc = document.createElement("p");
          desc.className = "meta-text";
          desc.textContent = field.description;
          li.appendChild(desc);
        }

        tableSchemaList.appendChild(li);
      }
    }

    renderTableData(tablePreviewTable, columns, rows);
    tablePreviewMeta.textContent = `preview rows: ${rows.length}`;
    updateTableActionState();
  } catch (err) {
    tableDetailsMeta.textContent = "table details unavailable";
    tableInfoName.textContent = "-";
    tableInfoDescription.textContent = "-";
    tableInfoETag.textContent = "-";
    tableInfoCreated.textContent = "-";
    tableInfoUpdated.textContent = "-";
    tableInfoLabels.textContent = "{}";
    if (tableFriendlyNameInput) {
      tableFriendlyNameInput.value = "";
    }
    if (tableDescriptionInput) {
      tableDescriptionInput.value = "";
    }
    tableSchemaList.innerHTML = "";
    const li = document.createElement("li");
    li.className = "schema-item";
    li.textContent = err.message;
    tableSchemaList.appendChild(li);
    renderTableData(tablePreviewTable, ["error"], [[err.message]]);
    tablePreviewMeta.textContent = "preview unavailable";
    tableDetailsJson.textContent = JSON.stringify({ error: err.message }, null, 2);
    updateTableActionState("table details unavailable");
  }
}

async function querySelectedTable() {
  if (!hasSelectedTable()) {
    return;
  }
  const projectId = getProjectId();
  queryText.value = `SELECT *\nFROM \`${projectId}.${selectedDatasetId}.${selectedTableId}\`\nLIMIT 100;`;
  setActiveMainTab("query-workspace");
  queryRunStatus.textContent = `query drafted for ${selectedDatasetId}.${selectedTableId}`;
}

async function copySelectedTable() {
  if (!hasSelectedTable()) {
    return;
  }
  const projectId = getProjectId();
  const destDataset = window.prompt("Destination dataset ID", selectedDatasetId);
  if (!destDataset) {
    return;
  }
  const defaultTarget = `${selectedTableId}_copy`;
  const destTable = window.prompt("Destination table ID", defaultTarget);
  if (!destTable) {
    return;
  }

  try {
    const payload = {
      configuration: {
        copy: {
          sourceTable: {
            projectId,
            datasetId: selectedDatasetId,
            tableId: selectedTableId,
          },
          destinationTable: {
            projectId,
            datasetId: destDataset,
            tableId: destTable,
          },
          writeDisposition: "WRITE_EMPTY",
        },
      },
    };

    const created = await fetchJson(`/api/bigquery/v2/projects/${encodeURIComponent(projectId)}/jobs`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const ref = created.jobReference || created.job?.jobReference || {};
    const jobId = ref.jobId || "unknown";
    updateTableActionState(`copy job submitted: ${jobId}`);
    setActiveMainTab("jobs-explorer");
    await loadJobs(projectId);
  } catch (err) {
    updateTableActionState("copy failed");
    alert(`Copy table failed: ${err.message}`);
  }
}

async function deleteSelectedTable() {
  if (!hasSelectedTable()) {
    return;
  }
  const projectId = getProjectId();
  const full = `${selectedDatasetId}.${selectedTableId}`;
  const ok = window.confirm(`Delete table ${full}?`);
  if (!ok) {
    return;
  }

  try {
    await fetchJson(`/api/bigquery/v2/projects/${encodeURIComponent(projectId)}/datasets/${encodeURIComponent(selectedDatasetId)}/tables/${encodeURIComponent(selectedTableId)}`, {
      method: "DELETE",
    });
    updateTableActionState(`table deleted: ${full}`);
    selectedTableId = "";
    await refreshAll();
  } catch (err) {
    updateTableActionState("delete failed");
    alert(`Delete table failed: ${err.message}`);
  }
}

async function updateSelectedTableMetadata() {
  if (!hasSelectedTable()) {
    return;
  }
  const projectId = getProjectId();
  try {
    await fetchJson(`/api/bigquery/v2/projects/${encodeURIComponent(projectId)}/datasets/${encodeURIComponent(selectedDatasetId)}/tables/${encodeURIComponent(selectedTableId)}`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        friendlyName: tableFriendlyNameInput ? tableFriendlyNameInput.value.trim() : "",
        description: tableDescriptionInput ? tableDescriptionInput.value.trim() : "",
      }),
    });
    updateTableActionState(`table metadata saved: ${selectedDatasetId}.${selectedTableId}`);
    await loadTableDetails(projectId, selectedDatasetId, selectedTableId);
    await renderExplorerTree(projectId);
  } catch (err) {
    updateTableActionState("table metadata update failed");
    alert(`Update table metadata failed: ${err.message}`);
  }
}

async function loadTablePreview(projectId, datasetId, tableId) {
  try {
    if (breadcrumbDatasetChip) {
      breadcrumbDatasetChip.textContent = datasetId;
    }
    if (breadcrumbTableChip) {
      breadcrumbTableChip.textContent = tableId;
    }
    const res = await fetchJson(`/api/bigquery/v2/projects/${encodeURIComponent(projectId)}/tabledata/${encodeURIComponent(datasetId)}/${encodeURIComponent(tableId)}/data?maxResults=25`);
    const rows = (res.rows || []).map((r) => (r.f || []).map((cell) => String(cell.v ?? "")));
    const cols = rows.length > 0 ? rows[0].map((_, idx) => `col_${idx + 1}`) : ["empty"];
    renderResultsGrid(cols, rows);
    queryResultsMeta.textContent = `preview ${datasetId}.${tableId} (${rows.length} rows)`;
  } catch (err) {
    renderResultsGrid(["error"], [[err.message]]);
    queryResultsMeta.textContent = "table preview failed";
  }
}

async function loadJobs(projectId) {
  const params = new URLSearchParams();
  params.set("maxResults", "10");
  if (jobsPageToken) {
    params.set("pageToken", jobsPageToken);
  }
  const stateFilter = jobsStateFilter.value.trim();
  if (stateFilter) {
    params.set("stateFilter", stateFilter);
  }
  const userEmail = jobsUserEmailFilter.value.trim();
  if (userEmail) {
    params.set("userEmail", userEmail);
  }
  if (allUsersToggle.checked) {
    params.set("allUsers", "true");
  }

  const data = await fetchJson(`/api/bigquery/v2/projects/${encodeURIComponent(projectId)}/jobs?${params.toString()}`);
  const rows = data.jobs || [];
  jobsNextPageToken = data.nextPageToken || "";
  lastJobsCount = rows.length;
  jobsPageHint.textContent = jobsPageToken ? `page: token ${jobsPageHistory.length + 1}` : "page: start";
  jobsPrevBtn.disabled = jobsPageHistory.length === 0;
  jobsNextBtn.disabled = !jobsNextPageToken;
  jobsStatus.textContent = `${rows.length} listed`;

  jobsList.innerHTML = "";
  if (!rows.length) {
    const li = document.createElement("li");
    li.textContent = "No data.";
    jobsList.appendChild(li);
    return;
  }

  for (const row of rows) {
    const ref = row.jobReference || {};
    const status = row.status || {};
    const id = ref.jobId || "";
    const li = document.createElement("li");
    li.textContent = `${id || "?"} - ${status.state || "?"}`;
    if (id && id === selectedJobId) {
      li.classList.add("active");
    }
    li.addEventListener("click", async () => {
      selectedJobId = id;
      updateSelectedJobHint();
      await loadJobDetails(projectId, id);
      await loadJobs(projectId);
    });
    jobsList.appendChild(li);
  }

  if (selectedJobId && !rows.some((r) => (r.jobReference || {}).jobId === selectedJobId)) {
    selectedJobHint.textContent = `selected: ${selectedJobId} (out of page)`;
  }
}

async function loadConfig() {
  const cfg = await fetchJson("/config");
  emulatorTarget.textContent = `emulator: ${cfg.emulator}`;
}

async function loadJobDetails(projectId, jobId) {
  if (!jobId) {
    jobDetailsJson.textContent = "{}";
    return;
  }
  try {
    const details = await fetchJson(`/api/bigquery/v2/projects/${encodeURIComponent(projectId)}/jobs/${encodeURIComponent(jobId)}`);
    jobDetailsJson.textContent = JSON.stringify(details, null, 2);
  } catch (err) {
    jobDetailsJson.textContent = JSON.stringify({ error: err.message }, null, 2);
  }
}

function renderResultsGrid(columns, rows) {
  renderTableData(queryResultsTable, columns, rows);
}

async function loadQueryResults(projectId, jobId) {
  if (!jobId) {
    renderResultsGrid(["result"], [["No query selected"]]);
    queryResultsMeta.textContent = "no results yet";
    queryResultsJson.textContent = "{}";
    queryResultsStats.textContent = "{}";
    return;
  }
  try {
    const res = await fetchJson(`/api/bigquery/v2/projects/${encodeURIComponent(projectId)}/jobs/${encodeURIComponent(jobId)}/queryResults?maxResults=50`);
    const fields = (((res.schema || {}).fields) || []).map((f) => f.name || "col");
    const rows = (res.rows || []).map((r) => (r.f || []).map((cell) => String(cell.v ?? "")));
    renderResultsGrid(fields, rows);
    queryResultsMeta.textContent = `rows: ${rows.length} / total: ${res.totalRows || rows.length}`;
    
    // Detailed views
    queryResultsJson.textContent = JSON.stringify(res, null, 2);
    
    // Fetch full job for stats
    const details = await fetchJson(`/api/bigquery/v2/projects/${encodeURIComponent(projectId)}/jobs/${encodeURIComponent(jobId)}`);
    queryResultsStats.textContent = JSON.stringify(details.statistics || {}, null, 2);

  } catch (err) {
    renderResultsGrid(["error"], [[err.message]]);
    queryResultsMeta.textContent = "query results unavailable";
    queryResultsJson.textContent = JSON.stringify({ error: err.message }, null, 2);
    queryResultsStats.textContent = "{}";
  }
}

function renderSavedQueries() {
  const items = getSavedQueries();
  savedQueriesList.innerHTML = "";
  if (!items.length) {
    const li = document.createElement("li");
    li.textContent = "No saved queries.";
    savedQueriesList.appendChild(li);
    return;
  }

  for (const item of items) {
    const activeVersion = item.versions[item.currentVersion] || item.versions[item.versions.length - 1];
    const li = document.createElement("li");
    const row = document.createElement("div");
    row.className = "row-actions";

    const label = document.createElement("span");
    label.textContent = `${item.name} (v${item.currentVersion + 1}/${item.versions.length})`;
    label.className = "meta-text";

    const loadBtn = document.createElement("button");
    loadBtn.type = "button";
    loadBtn.textContent = "Load";
    loadBtn.addEventListener("click", () => {
      queryText.value = activeVersion?.sql || "";
    });

    const prevBtn = document.createElement("button");
    prevBtn.type = "button";
    prevBtn.textContent = "Prev";
    prevBtn.disabled = item.currentVersion === 0;
    prevBtn.addEventListener("click", () => {
      item.currentVersion = Math.max(0, item.currentVersion - 1);
      setSavedQueries(items);
      renderSavedQueries();
    });

    const nextBtn = document.createElement("button");
    nextBtn.type = "button";
    nextBtn.textContent = "Next";
    nextBtn.disabled = item.currentVersion >= item.versions.length - 1;
    nextBtn.addEventListener("click", () => {
      item.currentVersion = Math.min(item.versions.length - 1, item.currentVersion + 1);
      setSavedQueries(items);
      renderSavedQueries();
    });

    const shareBtn = document.createElement("button");
    shareBtn.type = "button";
    shareBtn.textContent = "Share";
    shareBtn.addEventListener("click", async () => {
      try {
        await copyQueryShareLink(activeVersion?.sql || "");
        queryRunStatus.textContent = "query link copied";
      } catch (_) {
        queryRunStatus.textContent = "unable to copy link";
      }
    });

    const delBtn = document.createElement("button");
    delBtn.type = "button";
    delBtn.textContent = "Delete";
    delBtn.addEventListener("click", () => {
      const next = getSavedQueries().filter((q) => q.name !== item.name);
      setSavedQueries(next);
      renderSavedQueries();
    });

    row.appendChild(label);
    row.appendChild(loadBtn);
    row.appendChild(prevBtn);
    row.appendChild(nextBtn);
    row.appendChild(shareBtn);
    row.appendChild(delBtn);
    li.appendChild(row);
    savedQueriesList.appendChild(li);
  }
}

async function runQueryJob() {
  const projectId = getProjectId();
  const sql = queryText.value.trim();
  if (!sql) {
    queryRunStatus.textContent = "query required";
    return;
  }

  queryRunStatus.textContent = "submitting";
  const payload = {
    configuration: {
      query: {
        query: sql,
      },
    },
  };

  try {
    const created = await fetchJson(`/api/bigquery/v2/projects/${encodeURIComponent(projectId)}/jobs`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    const ref = created.jobReference || created.job?.jobReference || {};
    selectedJobId = ref.jobId || "";
    updateSelectedJobHint();
    queryRunStatus.textContent = selectedJobId ? `submitted ${selectedJobId}` : "submitted";
    await Promise.all([
      loadJobs(projectId),
      loadJobDetails(projectId, selectedJobId),
      loadQueryResults(projectId, selectedJobId),
    ]);
  } catch (err) {
    queryRunStatus.textContent = "submit failed";
    alert(`Run query failed: ${err.message}`);
  }
}

async function cancelSelectedJob() {
  const projectId = getProjectId();
  if (!selectedJobId) {
    return;
  }
  try {
    await fetchJson(`/api/bigquery/v2/projects/${encodeURIComponent(projectId)}/jobs/${encodeURIComponent(selectedJobId)}/cancel`, {
      method: "POST",
    });
    await Promise.all([
      loadJobs(projectId),
      loadJobDetails(projectId, selectedJobId),
      loadQueryResults(projectId, selectedJobId),
    ]);
  } catch (err) {
    alert(`Cancel failed: ${err.message}`);
  }
}

async function saveQueryShortcut() {
  const name = savedQueryName.value.trim() || `query-${Date.now()}`;
  const sql = queryText.value.trim();
  if (!sql) {
    queryRunStatus.textContent = "query required";
    return;
  }

  upsertSavedQuery(name, sql);
  savedQueryName.value = "";
  renderSavedQueries();
  queryRunStatus.textContent = "query saved";
}

queryText.addEventListener("keydown", async (event) => {
  if ((event.ctrlKey || event.metaKey) && event.key === "Enter") {
    event.preventDefault();
    await runQueryJob();
    return;
  }

  if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "s") {
    event.preventDefault();
    await saveQueryShortcut();
  }
});

async function refreshAll() {
  const projectId = getProjectId();
  updateProjectChip();
  try {
    await Promise.all([
      loadConfig(),
      loadHealth(),
      loadCapabilities(),
      loadDatasets(projectId),
      loadJobs(projectId),
    ]);
    await Promise.all([
      loadJobDetails(projectId, selectedJobId),
      loadQueryResults(projectId, selectedJobId),
    ]);
    if (selectedDatasetId && selectedTableId) {
      await Promise.all([
        loadTablePreview(projectId, selectedDatasetId, selectedTableId),
        loadTableDetails(projectId, selectedDatasetId, selectedTableId),
      ]);
    }
  } catch (err) {
    healthStatus.textContent = "error";
    healthStatus.className = "metric status-warn";
    jobsStatus.textContent = "check console";
    console.error(err);
  }
}

function setActiveMainTab(targetId) {
  if (!mainTabs) {
    return;
  }
  const tab = mainTabs.querySelector(`.tab[data-target="${targetId}"]`);
  if (!tab) {
    return;
  }

  mainTabs.querySelectorAll(".tab").forEach((t) => t.classList.remove("active"));
  tab.classList.add("active");

  ["query-workspace", "query-results-panel", "table-details-panel", "jobs-explorer", "details-section", "capabilities-view"].forEach((id) => {
    const section = document.getElementById(id);
    if (!section) {
      return;
    }

    let show = false;
    if (id === targetId) show = true;
    if (targetId === "query-workspace" && (id === "query-results-panel" || id === "table-details-panel")) show = true;
    if (targetId === "jobs-explorer" && id === "details-section") show = true;

    section.style.display = show ? "block" : "none";
  });
}

function setActiveRail(nav) {
  railIcons.forEach((btn) => {
    btn.classList.toggle("active", btn.dataset.nav === nav);
  });
}

function resetJobsPaging() {
  jobsPageToken = "";
  jobsNextPageToken = "";
  jobsPageHistory = [];
}

if (navCollapseBtn) {
  navCollapseBtn.addEventListener("click", () => {
    document.body.classList.toggle("nav-collapsed");
  });
}

if (projectSelectorBtn && projectInput) {
  projectSelectorBtn.addEventListener("click", () => {
    projectInput.focus();
    projectInput.select();
  });
}

if (appbarSearchBtn) {
  appbarSearchBtn.addEventListener("click", () => {
    globalSearchInput.focus();
    setActiveRail("search");
  });
}

if (appbarStarredBtn) {
  appbarStarredBtn.addEventListener("click", () => {
    savedQueryName.focus();
    queryRunStatus.textContent = "saved queries panel ready";
  });
}

if (appbarThemeBtn) {
  appbarThemeBtn.addEventListener("click", () => {
    const next = currentTheme() === "dark" ? "light" : "dark";
    applyTheme(next);
  });
}

if (appbarMoreBtn) {
  appbarMoreBtn.addEventListener("click", () => {
    setActiveMainTab("capabilities-view");
    setActiveRail("studio");
  });
}

for (const btn of railIcons) {
  btn.addEventListener("click", async () => {
    const nav = btn.dataset.nav || "studio";
    setActiveRail(nav);

    if (nav === "studio") {
      setActiveMainTab("query-workspace");
      return;
    }
    if (nav === "search") {
      setActiveMainTab("query-workspace");
      globalSearchInput.focus();
      return;
    }
    if (nav === "jobs") {
      setActiveMainTab("jobs-explorer");
      await loadJobs(getProjectId());
      return;
    }
    if (nav === "history") {
      allUsersToggle.checked = true;
      setActiveMainTab("jobs-explorer");
      await loadJobs(getProjectId());
      return;
    }
    if (nav === "settings") {
      const next = currentTheme() === "dark" ? "light" : "dark";
      applyTheme(next);
    }
  });
}

if (globalSearchInput) {
  globalSearchInput.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      event.preventDefault();
      explorerSearchInput.value = globalSearchInput.value;
      explorerFilterText = globalSearchInput.value || "";
      renderExplorerTree(getProjectId());
    }
  });
}

refreshBtn.addEventListener("click", refreshAll);
loadProjectBtn.addEventListener("click", async () => {
  resetJobsPaging();
  await refreshAll();
});
refreshJobBtn.addEventListener("click", async () => {
  await loadJobDetails(getProjectId(), selectedJobId);
  await loadJobs(getProjectId());
});
cancelJobBtn.addEventListener("click", cancelSelectedJob);

themeToggle.addEventListener("click", () => {
  const next = currentTheme() === "dark" ? "light" : "dark";
  applyTheme(next);
});

jobsFilterForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  resetJobsPaging();
  await loadJobs(getProjectId());
  await loadJobDetails(getProjectId(), selectedJobId);
});

saveQueryForm.addEventListener("submit", (event) => {
  event.preventDefault();
  const name = savedQueryName.value.trim();
  const sql = queryText.value.trim();
  if (!name || !sql) {
    return;
  }

  upsertSavedQuery(name, sql);
  savedQueryName.value = "";
  renderSavedQueries();
});

if (exportSavedQueriesBtn) {
  exportSavedQueriesBtn.addEventListener("click", () => {
    exportSavedQueries();
    queryRunStatus.textContent = "saved queries exported";
  });
}

if (importSavedQueriesBtn && savedQueriesImportInput) {
  importSavedQueriesBtn.addEventListener("click", () => {
    savedQueriesImportInput.click();
  });

  savedQueriesImportInput.addEventListener("change", async (event) => {
    const file = event.target.files && event.target.files[0];
    if (!file) {
      return;
    }
    try {
      await importSavedQueriesFromFile(file);
      renderSavedQueries();
      queryRunStatus.textContent = "saved queries imported";
    } catch (err) {
      queryRunStatus.textContent = "import failed";
      alert(`Import failed: ${err.message}`);
    } finally {
      savedQueriesImportInput.value = "";
    }
  });
}

clearJobsFiltersBtn.addEventListener("click", async () => {
  jobsStateFilter.value = "";
  jobsUserEmailFilter.value = "";
  allUsersToggle.checked = false;
  resetJobsPaging();
  await loadJobs(getProjectId());
  await loadJobDetails(getProjectId(), selectedJobId);
});

explorerSearchInput.addEventListener("input", async () => {
  explorerFilterText = explorerSearchInput.value || "";
  await renderExplorerTree(getProjectId());
});

clearExplorerSearchBtn.addEventListener("click", async () => {
  explorerFilterText = "";
  explorerSearchInput.value = "";
  await renderExplorerTree(getProjectId());
});

if (explorerCapabilityFilter) {
  explorerCapabilityFilter.addEventListener("change", async () => {
    explorerCapabilityFilterText = explorerCapabilityFilter.value || "all";
    localStorage.setItem(explorerCapabilityFilterStorageKey, explorerCapabilityFilterText);
    await renderExplorerTree(getProjectId());
  });
}

jobsNextBtn.addEventListener("click", async () => {
  if (!jobsNextPageToken) {
    return;
  }
  jobsPageHistory.push(jobsPageToken);
  jobsPageToken = jobsNextPageToken;
  await loadJobs(getProjectId());
  await loadJobDetails(getProjectId(), selectedJobId);
});

jobsPrevBtn.addEventListener("click", async () => {
  if (!jobsPageHistory.length) {
    return;
  }
  jobsPageToken = jobsPageHistory.pop() || "";
  await loadJobs(getProjectId());
  await Promise.all([
    loadJobDetails(getProjectId(), selectedJobId),
    loadQueryResults(getProjectId(), selectedJobId),
  ]);
});

runQueryForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  await runQueryJob();
});

createDatasetForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  const projectId = getProjectId();
  const datasetId = document.getElementById("newDatasetId").value.trim();
  if (!datasetId) {
    return;
  }

  try {
    await fetchJson(`/api/bigquery/v2/projects/${encodeURIComponent(projectId)}/datasets`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ datasetReference: { datasetId } }),
    });
    document.getElementById("newDatasetId").value = "";
    await refreshAll();
  } catch (err) {
    alert(`Create dataset failed: ${err.message}`);
  }
});

if (updateDatasetForm) {
  updateDatasetForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    const projectId = getProjectId();
    const datasetId = (datasetMetaDatasetId?.value || "").trim() || selectedDatasetId;
    if (!datasetId) {
      return;
    }

    try {
      let labels = {};
      if (datasetLabelsInput && datasetLabelsInput.value.trim()) {
        try {
          labels = JSON.parse(datasetLabelsInput.value.trim());
        } catch (e) {
          alert("Invalid labels JSON. Use format: {\"key\": \"value\"}");
          return;
        }
      }

      await fetchJson(`/api/bigquery/v2/projects/${encodeURIComponent(projectId)}/datasets/${encodeURIComponent(datasetId)}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          friendlyName: datasetFriendlyNameInput ? datasetFriendlyNameInput.value.trim() : "",
          location: datasetLocationInput ? datasetLocationInput.value.trim() : "",
          labels: labels,
        }),
      });
      selectedDatasetId = datasetId;
      await refreshAll();
      await selectDataset(projectId, datasetId);
    } catch (err) {
      alert(`Update dataset failed: ${err.message}`);
    }
  });
}

if (deleteDatasetBtn) {
  deleteDatasetBtn.addEventListener("click", async () => {
    const projectId = getProjectId();
    const datasetId = (datasetMetaDatasetId?.value || "").trim() || selectedDatasetId;
    if (!datasetId) {
      return;
    }
    const ok = window.confirm(`Delete dataset ${datasetId}?`);
    if (!ok) {
      return;
    }
    try {
      await fetchJson(`/api/bigquery/v2/projects/${encodeURIComponent(projectId)}/datasets/${encodeURIComponent(datasetId)}`, {
        method: "DELETE",
      });
      if (selectedDatasetId === datasetId) {
        selectedDatasetId = "";
        selectedTableId = "";
      }
      await refreshAll();
      syncDatasetMetaInputs();
    } catch (err) {
      alert(`Delete dataset failed: ${err.message}`);
    }
  });
}

if (createTableForm) {
  createTableForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    const projectId = getProjectId();
    const datasetId = document.getElementById("newTableDatasetId").value.trim() || selectedDatasetId || "analytics";
    const tableId = document.getElementById("newTableId").value.trim();
    if (!datasetId || !tableId) {
      return;
    }

    try {
      await fetchJson(`/api/bigquery/v2/projects/${encodeURIComponent(projectId)}/datasets/${encodeURIComponent(datasetId)}/tables`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ tableReference: { tableId } }),
      });
      document.getElementById("newTableDatasetId").value = datasetId;
      document.getElementById("newTableId").value = "";
      selectedDatasetId = datasetId;
      await refreshAll();
      await selectTable(projectId, datasetId, tableId);
    } catch (err) {
      alert(`Create table failed: ${err.message}`);
    }
  });
}

if (updateTableMetaForm) {
  updateTableMetaForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    await updateSelectedTableMetadata();
  });
}

if (updateTableLabelsBtn) {
  updateTableLabelsBtn.addEventListener("click", async () => {
    if (!hasSelectedTable()) return;
    const projectId = getProjectId();
    let labels = {};
    if (tableLabelsInput && tableLabelsInput.value.trim()) {
      try {
        labels = JSON.parse(tableLabelsInput.value.trim());
      } catch (e) {
        alert("Invalid labels JSON. Use format: {\"key\": \"value\"}");
        return;
      }
    }
    try {
      await fetchJson(`/api/bigquery/v2/projects/${encodeURIComponent(projectId)}/datasets/${encodeURIComponent(selectedDatasetId)}/tables/${encodeURIComponent(selectedTableId)}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ labels }),
      });
      await loadTableDetails(projectId, selectedDatasetId, selectedTableId);
    } catch (err) {
      alert(`Update labels failed: ${err.message}`);
    }
  });
}

if (mainTabs) {
  mainTabs.addEventListener("click", (e) => {
    const tab = e.target.closest(".tab");
    if (!tab) return;
    const targetId = tab.getAttribute("data-target");
    setActiveMainTab(targetId);
  });

  // Default view
  setActiveMainTab("query-workspace");
}

if (resultTabs) {
  resultTabs.addEventListener("click", (e) => {
    const tab = e.target.closest(".tab");
    if (!tab) return;

    resultTabs.querySelectorAll(".tab").forEach((t) => t.classList.remove("active"));
    tab.classList.add("active");

    const targetId = tab.getAttribute("data-target");
    ["result-table-view", "result-json-view", "result-stats-view"].forEach((id) => {
      const el = document.getElementById(id);
      if (el) {
        el.style.display = id === targetId ? "block" : "none";
      }
    });
  });
}

if (tableDetailsTabs) {
  tableDetailsTabs.addEventListener("click", (e) => {
    const tab = e.target.closest(".tab");
    if (!tab) return;

    tableDetailsTabs.querySelectorAll(".tab").forEach((t) => t.classList.remove("active"));
    tab.classList.add("active");

    const targetId = tab.getAttribute("data-target");
    ["table-overview-view", "table-schema-view", "table-preview-view", "table-json-view"].forEach((id) => {
      const el = document.getElementById(id);
      if (el) {
        el.style.display = id === targetId ? "block" : "none";
      }
    });
  });
}

if (jobsHistoryTabs) {
  jobsHistoryTabs.addEventListener("click", async (e) => {
    const tab = e.target.closest(".tab");
    if (!tab) return;

    jobsHistoryTabs.querySelectorAll(".tab").forEach((t) => t.classList.remove("active"));
    tab.classList.add("active");

    const target = tab.getAttribute("data-target");
    const projectHistory = target === "project-history";
    allUsersToggle.checked = projectHistory;
    jobsStateFilter.value = projectHistory ? "" : jobsStateFilter.value;
    if (jobsHistoryHint) {
      jobsHistoryHint.textContent = projectHistory
        ? "Scope: all users in current project"
        : "Scope: personal jobs in current project";
    }
    resetJobsPaging();
    await loadJobs(getProjectId());
    await loadJobDetails(getProjectId(), selectedJobId);
  });
}

if (queryTableBtn) {
  queryTableBtn.addEventListener("click", querySelectedTable);
}

if (copyTableBtn) {
  copyTableBtn.addEventListener("click", copySelectedTable);
}

if (deleteTableBtn) {
  deleteTableBtn.addEventListener("click", deleteSelectedTable);
}

if (datasetCopyIdBtn) {
  datasetCopyIdBtn.addEventListener("click", copyDatasetId);
}

if (datasetListTablesBtn) {
  datasetListTablesBtn.addEventListener("click", listSelectedDatasetTables);
}

if (datasetQueryBtn) {
  datasetQueryBtn.addEventListener("click", querySelectedDataset);
}

updateSelectedJobHint();
jobDetailsJson.textContent = "{}";
jobsPrevBtn.disabled = true;
jobsNextBtn.disabled = true;
jobsPageHint.textContent = "page: start";
const initialTheme = localStorage.getItem(themeStorageKey) || "light";
applyTheme(initialTheme);
if (explorerCapabilityFilter) {
  const storedFilter = localStorage.getItem(explorerCapabilityFilterStorageKey) || "all";
  explorerCapabilityFilterText = storedFilter;
  explorerCapabilityFilter.value = storedFilter;
}
updateProjectChip();
syncCreateTableDatasetInput();
syncDatasetMetaInputs();
updateTableActionState();
if (jobsHistoryHint) {
  jobsHistoryHint.textContent = "Scope: personal jobs in current project";
}
loadQueryFromURL();
renderSavedQueries();
refreshAll();
setInterval(refreshAll, 5000);
