const projectInput = document.getElementById("projectId");
const refreshBtn = document.getElementById("refreshBtn");
const loadProjectBtn = document.getElementById("loadProjectBtn");
const themeToggle = document.getElementById("themeToggle");
const createDatasetForm = document.getElementById("createDatasetForm");
const saveQueryForm = document.getElementById("saveQueryForm");
const savedQueryName = document.getElementById("savedQueryName");
const runQueryForm = document.getElementById("runQueryForm");
const queryText = document.getElementById("queryText");
const queryRunStatus = document.getElementById("queryRunStatus");
const queryResultsMeta = document.getElementById("queryResultsMeta");
const queryResultsTable = document.getElementById("queryResultsTable");
const refreshJobBtn = document.getElementById("refreshJobBtn");
const cancelJobBtn = document.getElementById("cancelJobBtn");
const selectedJobHint = document.getElementById("selectedJobHint");
const jobDetailsJson = document.getElementById("jobDetailsJson");
const jobsFilterForm = document.getElementById("jobsFilterForm");
const jobsStateFilter = document.getElementById("jobsStateFilter");
const jobsUserEmailFilter = document.getElementById("jobsUserEmailFilter");
const clearJobsFiltersBtn = document.getElementById("clearJobsFiltersBtn");
const jobsPrevBtn = document.getElementById("jobsPrevBtn");
const jobsNextBtn = document.getElementById("jobsNextBtn");
const jobsPageHint = document.getElementById("jobsPageHint");

const healthStatus = document.getElementById("healthStatus");
const capabilitiesStatus = document.getElementById("capabilitiesStatus");
const jobsStatus = document.getElementById("jobsStatus");
const emulatorTarget = document.getElementById("emulatorTarget");
const explorerTree = document.getElementById("explorerTree");
const jobsList = document.getElementById("jobsList");
const capabilitiesJson = document.getElementById("capabilitiesJson");
const savedQueriesList = document.getElementById("savedQueriesList");

let selectedJobId = "";
let selectedDatasetId = "";
let selectedTableId = "";
let jobsPageToken = "";
let jobsNextPageToken = "";
let jobsPageHistory = [];
let lastJobsCount = 0;

const savedQueriesStorageKey = "locaql.savedQueries";
const themeStorageKey = "locaql.theme";

async function fetchJson(path, options) {
  const res = await fetch(path, options);
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`${res.status} ${res.statusText}: ${body}`);
  }
  return res.json();
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

function getSavedQueries() {
  try {
    const raw = localStorage.getItem(savedQueriesStorageKey);
    if (!raw) {
      return [];
    }
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : [];
  } catch (_) {
    return [];
  }
}

function setSavedQueries(items) {
  localStorage.setItem(savedQueriesStorageKey, JSON.stringify(items));
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
  const supported = entries.filter(([, v]) => v.status === "supported").length;
  const partial = entries.filter(([, v]) => v.status === "partial").length;
  capabilitiesStatus.textContent = `${supported} supported / ${partial} partial`;
  capabilitiesJson.textContent = JSON.stringify(caps, null, 2);
}

async function loadDatasets(projectId) {
  const data = await fetchJson(`/api/bigquery/v2/projects/${encodeURIComponent(projectId)}/datasets?maxResults=50`);
  const rows = data.datasets || [];
  await renderExplorerTree(projectId, rows);
}

async function renderExplorerTree(projectId, datasets) {
  explorerTree.innerHTML = "";
  const projectNode = document.createElement("div");
  projectNode.className = "node project";
  projectNode.textContent = `Project: ${projectId}`;
  explorerTree.appendChild(projectNode);

  for (const ds of datasets) {
    const dsRef = ds.datasetReference || {};
    const datasetId = dsRef.datasetId || "";

    const datasetNode = document.createElement("div");
    datasetNode.className = "node dataset";
    datasetNode.textContent = `Dataset: ${datasetId}`;
    explorerTree.appendChild(datasetNode);

    if (!datasetId) {
      continue;
    }

    let tables = [];
    try {
      const tablesResp = await fetchJson(`/api/bigquery/v2/projects/${encodeURIComponent(projectId)}/datasets/${encodeURIComponent(datasetId)}/tables?maxResults=20`);
      tables = tablesResp.tables || [];
    } catch (_) {
      tables = [];
    }

    for (const t of tables) {
      const tRef = t.tableReference || {};
      const tableId = tRef.tableId || "";
      const tableNode = document.createElement("div");
      tableNode.className = "node table";
      if (datasetId === selectedDatasetId && tableId === selectedTableId) {
        tableNode.classList.add("active");
      }
      tableNode.textContent = `Table: ${tableId}`;
      tableNode.addEventListener("click", async () => {
        selectedDatasetId = datasetId;
        selectedTableId = tableId;
        await renderExplorerTree(projectId, datasets);
        await loadTablePreview(projectId, datasetId, tableId);
      });
      explorerTree.appendChild(tableNode);
    }
  }
}

async function loadTablePreview(projectId, datasetId, tableId) {
  try {
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
  queryResultsTable.innerHTML = "";
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

  queryResultsTable.appendChild(thead);
  queryResultsTable.appendChild(tbody);
}

async function loadQueryResults(projectId, jobId) {
  if (!jobId) {
    renderResultsGrid(["result"], [["No query selected"]]);
    queryResultsMeta.textContent = "no results yet";
    return;
  }
  try {
    const res = await fetchJson(`/api/bigquery/v2/projects/${encodeURIComponent(projectId)}/jobs/${encodeURIComponent(jobId)}/queryResults?maxResults=50`);
    const fields = (((res.schema || {}).fields) || []).map((f) => f.name || "col");
    const rows = (res.rows || []).map((r) => (r.f || []).map((cell) => String(cell.v ?? "")));
    renderResultsGrid(fields, rows);
    queryResultsMeta.textContent = `rows: ${rows.length} / total: ${res.totalRows || rows.length}`;
  } catch (err) {
    renderResultsGrid(["error"], [[err.message]]);
    queryResultsMeta.textContent = "query results unavailable";
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
    const li = document.createElement("li");
    const row = document.createElement("div");
    row.className = "row-actions";

    const label = document.createElement("span");
    label.textContent = item.name;
    label.className = "meta-text";

    const loadBtn = document.createElement("button");
    loadBtn.type = "button";
    loadBtn.textContent = "Load";
    loadBtn.addEventListener("click", () => {
      queryText.value = item.sql || "";
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

async function refreshAll() {
  const projectId = getProjectId();
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
  } catch (err) {
    healthStatus.textContent = "error";
    healthStatus.className = "metric status-warn";
    jobsStatus.textContent = "check console";
    console.error(err);
  }
}

function resetJobsPaging() {
  jobsPageToken = "";
  jobsNextPageToken = "";
  jobsPageHistory = [];
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

  const items = getSavedQueries().filter((q) => q.name !== name);
  items.unshift({ name, sql, savedAt: Date.now() });
  setSavedQueries(items.slice(0, 20));
  savedQueryName.value = "";
  renderSavedQueries();
});

clearJobsFiltersBtn.addEventListener("click", async () => {
  jobsStateFilter.value = "";
  jobsUserEmailFilter.value = "";
  resetJobsPaging();
  await loadJobs(getProjectId());
  await loadJobDetails(getProjectId(), selectedJobId);
});

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

updateSelectedJobHint();
jobDetailsJson.textContent = "{}";
jobsPrevBtn.disabled = true;
jobsNextBtn.disabled = true;
jobsPageHint.textContent = "page: start";
const initialTheme = localStorage.getItem(themeStorageKey) || "light";
applyTheme(initialTheme);
renderSavedQueries();
refreshAll();
setInterval(refreshAll, 5000);
