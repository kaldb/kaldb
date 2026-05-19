(function () {
  "use strict";

  var API_BASE = "/slack.proto.astra.ManagerApiService";
  var MAX_TIME = "9223372036854775807"; // Long.MAX_VALUE as string

  var datasetsGrid = null;
  var datasetByName = {};
  var datasetGridActionsBound = false;

  var partitionsGrid = null;
  var partitionById = {};
  var partitionGridActionsBound = false;

  var redactionsGrid = null;
  var redactionByName = {};
  var redactionGridActionsBound = false;

  var partitionCatalogSupport = {
    status: "unknown",
    lastError: "",
    probePromise: null,
  };

  // ---- API helper ----

  function apiCall(method, body) {
    return fetch(API_BASE + "/" + method, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body || {}),
    }).then(function (res) {
      return res.text().then(function (text) {
        var data = {};

        if (text) {
          try {
            data = JSON.parse(text);
          } catch (e) {
            data = { raw: text };
          }
        }

        if (!res.ok) {
          throw new Error(data.message || data.error || data.raw || res.statusText);
        }
        return data;
      });
    });
  }

  // ---- Toast notifications ----

  function showToast(message, type) {
    var container = document.getElementById("toast-container");
    var toast = document.createElement("div");
    toast.className = "toast toast-" + (type || "success");
    toast.textContent = message;
    container.appendChild(toast);
    setTimeout(function () {
      toast.remove();
    }, 4000);
  }

  // ---- Modal helpers ----

  function openModal(id) {
    document.getElementById(id).classList.add("open");
  }

  function closeModal(id) {
    document.getElementById(id).classList.remove("open");
  }

  function closeAllModals() {
    var overlays = document.querySelectorAll(".modal-overlay");
    for (var i = 0; i < overlays.length; i++) {
      overlays[i].classList.remove("open");
    }
  }

  // ---- Tab switching ----

  function loadTabData(target) {
    if (target === "datasets") loadDatasets();
    if (target === "partitions") loadPartitions();
    if (target === "redactions") loadRedactions();
  }

  function initTabs() {
    var tabs = document.querySelectorAll(".tab");
    for (var i = 0; i < tabs.length; i++) {
      tabs[i].addEventListener("click", function () {
        var target = this.getAttribute("data-tab");
        var allTabs = document.querySelectorAll(".tab");
        var allContent = document.querySelectorAll(".tab-content");
        var j;

        for (j = 0; j < allTabs.length; j++) {
          allTabs[j].classList.remove("active");
        }
        for (j = 0; j < allContent.length; j++) {
          allContent[j].classList.remove("active");
        }

        this.classList.add("active");
        document.getElementById(target).classList.add("active");
        loadTabData(target);
      });
    }
  }

  function isTabActive(tabId) {
    var tab = document.getElementById(tabId);
    return !!tab && tab.classList.contains("active");
  }

  // ---- Confirm dialog ----

  var confirmCallback = null;

  function showConfirm(message, onConfirm) {
    document.getElementById("confirm-message").textContent = message;
    confirmCallback = onConfirm;
    openModal("modal-confirm");
  }

  function initConfirmDialog() {
    document.getElementById("confirm-ok").addEventListener("click", function () {
      closeModal("modal-confirm");
      if (confirmCallback) confirmCallback();
      confirmCallback = null;
    });
    document.getElementById("confirm-cancel").addEventListener("click", function () {
      closeModal("modal-confirm");
      confirmCallback = null;
    });
  }

  // ---- Danger confirm dialog (type-to-confirm) ----

  var dangerConfirmCallback = null;
  var dangerConfirmExpected = "";

  function showDangerConfirm(message, expectedText, onConfirm) {
    document.getElementById("danger-confirm-message").textContent = message;
    dangerConfirmExpected = expectedText;
    dangerConfirmCallback = onConfirm;
    var input = document.getElementById("danger-confirm-input");
    var btn = document.getElementById("danger-confirm-ok");
    var hint = document.getElementById("danger-confirm-hint");
    input.value = "";
    btn.disabled = true;
    hint.textContent = 'Type "' + expectedText + '" to enable the delete button.';
    openModal("modal-danger-confirm");
    setTimeout(function () {
      input.focus();
    }, 100);
  }

  function initDangerConfirmDialog() {
    var input = document.getElementById("danger-confirm-input");
    var btn = document.getElementById("danger-confirm-ok");

    input.addEventListener("input", function () {
      btn.disabled = input.value !== dangerConfirmExpected;
    });

    btn.addEventListener("click", function () {
      if (input.value !== dangerConfirmExpected) return;
      closeModal("modal-danger-confirm");
      if (dangerConfirmCallback) dangerConfirmCallback();
      dangerConfirmCallback = null;
      dangerConfirmExpected = "";
    });

    document.getElementById("danger-confirm-cancel").addEventListener("click", function () {
      closeModal("modal-danger-confirm");
      dangerConfirmCallback = null;
      dangerConfirmExpected = "";
    });
  }

  // ---- Utilities ----

  function escapeHtml(value) {
    return String(value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function gridColumn(label, tooltip, options) {
    var column = {};
    var key;

    if (options) {
      for (key in options) {
        if (Object.prototype.hasOwnProperty.call(options, key)) {
          column[key] = options[key];
        }
      }
    }

    column.name = label;
    column.attributes = function (cell, row) {
      if (row) return {};
      return {
        title: tooltip,
        "aria-label": label + ": " + tooltip,
      };
    };
    return column;
  }

  function formatTime(epochMs) {
    if (!epochMs || epochMs === "0") return "-";
    if (String(epochMs) === MAX_TIME) return "MAX";
    try {
      return new Date(Number(epochMs)).toISOString().replace("T", " ").replace(/\.000Z$/, " UTC");
    } catch (e) {
      return String(epochMs);
    }
  }

  function toDisplayNumber(value) {
    if (value == null || value === "") return "0";
    var num = Number(value);
    return Number.isFinite(num) ? String(num) : String(value);
  }

  function toNumericValue(value) {
    var num = Number(value);
    return Number.isFinite(num) ? num : 0;
  }

  function formatWithSeparators(value) {
    var num = Number(value);
    return Number.isFinite(num) ? num.toLocaleString() : String(value);
  }

  function isCanonicalNonNegativeIntegerString(value) {
    return /^(0|[1-9]\d*)$/.test(String(value));
  }

  function splitPartitionIds(raw) {
    if (!raw) return [];
    return raw
      .split(",")
      .map(function (item) {
        return item.trim();
      })
      .filter(function (item) {
        return item.length > 0;
      });
  }

  function duplicateValues(values) {
    var seen = {};
    var duplicates = {};
    var i;

    for (i = 0; i < values.length; i++) {
      if (seen[values[i]]) {
        duplicates[values[i]] = true;
      }
      seen[values[i]] = true;
    }

    return Object.keys(duplicates).sort();
  }

  function getActivePartitionConfig(partitionConfigs) {
    if (!partitionConfigs || partitionConfigs.length === 0) return null;

    var i;
    var active = null;
    var latestStart = -1;

    for (i = 0; i < partitionConfigs.length; i++) {
      if (String(partitionConfigs[i].endTimeEpochMs) === MAX_TIME) {
        return partitionConfigs[i];
      }
      if (toNumericValue(partitionConfigs[i].startTimeEpochMs) >= latestStart) {
        latestStart = toNumericValue(partitionConfigs[i].startTimeEpochMs);
        active = partitionConfigs[i];
      }
    }

    return active;
  }

  function getActivePartitionIds(partitionConfigs) {
    var active = getActivePartitionConfig(partitionConfigs);
    return active && active.partitions ? active.partitions.slice() : [];
  }

  function formatActivePartitionIds(partitionConfigs) {
    var ids = getActivePartitionIds(partitionConfigs);
    return ids.length ? ids.join(", ") : "(none)";
  }

  function formatCurrentWindow(partitionConfigs) {
    var active = getActivePartitionConfig(partitionConfigs);
    if (!active) return "-";
    return formatTime(active.startTimeEpochMs) + " -> " + formatTime(active.endTimeEpochMs);
  }

  function renderDatasetAssignmentHistory(dataset) {
    var container = document.getElementById("dataset-assignment-history");
    if (!container) return;

    if (!dataset) {
      container.innerHTML =
        '<p class="empty-inline">Assignment history appears after the dataset is saved.</p>';
      return;
    }

    var partitionConfigs = (dataset.partitionConfigs || []).slice();
    if (partitionConfigs.length === 0) {
      container.innerHTML = '<p class="empty-inline">No assignment history.</p>';
      return;
    }

    partitionConfigs.sort(function (a, b) {
      return toNumericValue(b.startTimeEpochMs) - toNumericValue(a.startTimeEpochMs);
    });

    var html =
      '<table class="compact-table">' +
      "<thead><tr>" +
      '<th title="Whether this assignment window is current or historical.">Status</th>' +
      '<th title="Start and end time for this assignment window.">Window</th>' +
      '<th title="Partition IDs assigned during this window.">Partition IDs</th>' +
      '<th title="Number of partitions assigned during this window.">Count</th>' +
      "</tr></thead><tbody>";
    var i;

    for (i = 0; i < partitionConfigs.length; i++) {
      var config = partitionConfigs[i];
      var partitions = config.partitions || [];
      var status = String(config.endTimeEpochMs) === MAX_TIME ? "Current" : "Historical";
      html +=
        "<tr>" +
        "<td>" +
        escapeHtml(status) +
        "</td>" +
        "<td>" +
        escapeHtml(formatTime(config.startTimeEpochMs) + " -> " + formatTime(config.endTimeEpochMs)) +
        "</td>" +
        "<td>" +
        escapeHtml(partitions.length ? partitions.join(", ") : "(none)") +
        "</td>" +
        "<td>" +
        escapeHtml(String(partitions.length)) +
        "</td>" +
        "</tr>";
    }

    container.innerHTML = html + "</tbody></table>";
  }

  function currentDatasetModeLabel(dataset) {
    return dataset && dataset.usingDedicatedPartitions ? "Dedicated" : "Shared";
  }

  function modeBadgeHtml(modeLabel) {
    var cssClass = modeLabel === "Dedicated" ? "mode-dedicated" : "mode-shared";
    return '<span class="mode-badge ' + cssClass + '">' + escapeHtml(modeLabel) + "</span>";
  }

  function refreshRelatedViews() {
    loadDatasets();
    if (partitionCatalogSupport.status === "available" || isTabActive("partitions")) {
      loadPartitions();
    }
  }

  // ---- Partition catalog capability ----

  function setPartitionCatalogSupport(status, errorMessage) {
    partitionCatalogSupport.status = status;
    partitionCatalogSupport.lastError = errorMessage || "";
    updateDatasetCapabilityNote();
  }

  function fetchPartitionMetadata() {
    return apiCall("ListPartitionMetadata", {}).then(
      function (data) {
        setPartitionCatalogSupport("available", "");
        return data.partitionMetadata || [];
      },
      function (err) {
        setPartitionCatalogSupport("unavailable", err.message || "Unknown error");
        throw err;
      }
    );
  }

  function probePartitionCatalogSupport(force) {
    if (!force) {
      if (partitionCatalogSupport.status === "available") return Promise.resolve(true);
      if (partitionCatalogSupport.status === "unavailable") return Promise.resolve(false);
      if (partitionCatalogSupport.probePromise) return partitionCatalogSupport.probePromise;
    }

    partitionCatalogSupport.probePromise = fetchPartitionMetadata().then(
      function () {
        partitionCatalogSupport.probePromise = null;
        return true;
      },
      function () {
        partitionCatalogSupport.probePromise = null;
        return false;
      }
    );

    return partitionCatalogSupport.probePromise;
  }

  // ---- Datasets ----

  function loadDatasets() {
    apiCall("ListDatasetMetadata", {})
      .then(function (data) {
        renderDatasets(data.datasetMetadata || []);
      })
      .catch(function (err) {
        showToast("Failed to load datasets: " + err.message, "error");
      });
  }

  function renderDatasetActions(datasetName) {
    var encodedName = encodeURIComponent(datasetName);
    return gridjs.html(
      '<div class="grid-actions">' +
      '<button class="btn btn-sm" data-action="edit" data-dataset="' + encodedName + '">Edit</button>' +
      '<button class="btn btn-sm btn-danger" data-action="delete" data-dataset="' + encodedName + '">Delete</button>' +
      "</div>"
    );
  }

  function bindDatasetGridActions() {
    if (datasetGridActionsBound) return;

    var container = document.getElementById("datasets-grid");
    if (!container) return;

    container.addEventListener("click", function (e) {
      var target = e.target.closest("[data-action][data-dataset]");
      if (!target) return;

      var action = target.getAttribute("data-action");
      var datasetName = decodeURIComponent(target.getAttribute("data-dataset") || "");
      var dataset = datasetByName[datasetName];
      if (!dataset) return;

      if (action === "edit") {
        showDatasetForm(dataset);
        return;
      }

      if (action === "delete") {
        showDangerConfirm(
          'You are about to permanently delete the dataset "' + dataset.name +
            '". This will remove all metadata, partition assignments, and throughput configuration. This action cannot be undone.',
          dataset.name,
          function () {
            apiCall("DeleteDatasetMetadata", { name: dataset.name })
              .then(function () {
                showToast("Dataset deleted");
                refreshRelatedViews();
              })
              .catch(function (err) {
                showToast("Error: " + err.message, "error");
              });
          }
        );
      }
    });

    datasetGridActionsBound = true;
  }

  function renderDatasets(datasets) {
    if (typeof gridjs === "undefined") {
      showToast("gridjs is not available", "error");
      return;
    }

    datasetByName = {};
    var rows = [];
    var i;

    for (i = 0; i < datasets.length; i++) {
      var ds = datasets[i];
      var partitionConfigs = ds.partitionConfigs || [];
      var activeIds = getActivePartitionIds(partitionConfigs);

      datasetByName[ds.name] = ds;
      rows.push([
        ds.name,
        ds.owner || "",
        ds.serviceNamePattern || "",
        toDisplayNumber(ds.throughputBytes || 0),
        gridjs.html(modeBadgeHtml(currentDatasetModeLabel(ds))),
        String(activeIds.length),
        activeIds.length ? activeIds.join(", ") : "(none)",
        formatCurrentWindow(partitionConfigs),
        String(partitionConfigs.length),
        renderDatasetActions(ds.name),
      ]);
    }

    if (datasetsGrid) {
      datasetsGrid.updateConfig({ data: rows }).forceRender();
      return;
    }

    datasetsGrid = new gridjs.Grid({
      columns: [
        gridColumn("Name", "Dataset identifier."),
        gridColumn("Owner", "Team or oncall owner responsible for this dataset."),
        gridColumn("Service Pattern", "Service-name match pattern used by dataset routing."),
        gridColumn("Throughput", "Allowed ingest throughput in bytes/sec."),
        gridColumn("Mode", "Whether the current assignment uses dedicated or shared partitions."),
        gridColumn("Active", "Number of partitions in the active assignment."),
        gridColumn("Active IDs", "Partition IDs in the active assignment."),
        gridColumn("Current", "Time window for the active assignment."),
        gridColumn("History", "Number of assignment windows retained for this dataset."),
        gridColumn("Actions", "Edit or delete this dataset.", { sort: false }),
      ],
      data: rows,
      search: true,
      sort: true,
      pagination: {
        enabled: true,
        limit: 20,
      },
    });

    datasetsGrid.render(document.getElementById("datasets-grid"));
    bindDatasetGridActions();
  }

  function showDatasetList() {
    document.getElementById("datasets-grid").style.display = "";
    document.querySelector("#datasets .toolbar").style.display = "";
    document.getElementById("dataset-form-page").style.display = "none";
    loadDatasets();
  }

  function showDatasetForm(dataset) {
    document.getElementById("datasets-grid").style.display = "none";
    document.querySelector("#datasets .toolbar").style.display = "none";
    document.getElementById("dataset-form-page").style.display = "";

    var form = document.getElementById("form-dataset");
    var title = document.getElementById("dataset-form-title");
    form.reset();

    if (dataset) {
      var activeIds = getActivePartitionIds(dataset.partitionConfigs || []);
      title.textContent = "Edit Dataset";
      form.elements.name.value = dataset.name;
      form.elements.name.readOnly = true;
      form.elements.owner.value = dataset.owner || "";
      form.elements.service_name_pattern.value = dataset.serviceNamePattern || "";
      form.elements.throughput_bytes.value = dataset.throughputBytes == null ? 0 : dataset.throughputBytes;
      form.elements.partition_mode.value = dataset.usingDedicatedPartitions ? "dedicated" : "shared";
      form.elements.assignment_strategy.value = "auto";
      form.elements.partition_ids.value = activeIds.join(", ");
      form.dataset.editing = "true";
      renderDatasetAssignmentHistory(dataset);
    } else {
      title.textContent = "New Dataset";
      form.elements.name.readOnly = false;
      form.elements.throughput_bytes.value = 0;
      form.elements.partition_mode.value = "dedicated";
      form.elements.assignment_strategy.value = "auto";
      form.elements.partition_ids.value = "";
      form.dataset.editing = "false";
      renderDatasetAssignmentHistory(null);
    }

    updateDatasetFormState();
    probePartitionCatalogSupport();
  }

  function updateDatasetCapabilityNote() {
    var note = document.getElementById("dataset-capability-note");
    var form = document.getElementById("form-dataset");
    if (!note || !form) return;

    var messages = [];
    var strategy = form.elements.assignment_strategy.value;
    var mode = form.elements.partition_mode.value;

    if (partitionCatalogSupport.status === "unknown") {
      if (strategy === "auto" || mode === "shared" || mode === "dedicated") {
        messages.push("Checking partition catalog support for quota and shard assignment features.");
      }
    } else if (partitionCatalogSupport.status === "unavailable") {
      if (strategy === "auto") {
        messages.push("Auto-assignment requires a manager build with partition catalog support.");
      }
      if (mode === "shared" || mode === "dedicated") {
        messages.push("Shared and dedicated mode choices require shard assignment support.");
      }
      if (partitionCatalogSupport.lastError) {
        messages.push("Latest manager response: " + partitionCatalogSupport.lastError);
      }
    }

    if (messages.length === 0) {
      note.hidden = true;
      note.textContent = "";
      return;
    }

    note.hidden = false;
    note.textContent = messages.join(" ");
  }

  function updateDatasetFormState() {
    var form = document.getElementById("form-dataset");
    var idsField = document.getElementById("dataset-partition-ids-field");
    if (!form || !idsField) return;

    idsField.style.display = form.elements.assignment_strategy.value === "manual" ? "" : "none";
    updateDatasetCapabilityNote();
  }

  function validateManualPartitionIds(partitionIds) {
    var duplicates = duplicateValues(partitionIds);
    if (duplicates.length) {
      return "Partition IDs must be unique: " + duplicates.join(", ");
    }

    var invalid = [];
    var i;
    for (i = 0; i < partitionIds.length; i++) {
      if (!isCanonicalNonNegativeIntegerString(partitionIds[i])) {
        invalid.push(partitionIds[i]);
      }
    }

    if (invalid.length) {
      invalid.sort();
      return "Partition IDs must be canonical non-negative integers: " + invalid.join(", ");
    }

    return null;
  }

  function parseDatasetForm(form) {
    var throughputInput = form.elements.throughput_bytes.value.trim();
    var parsedThroughput = Number(throughputInput);
    var assignmentStrategy = form.elements.assignment_strategy.value;
    var partitionMode = form.elements.partition_mode.value;
    var ids = assignmentStrategy === "manual"
      ? splitPartitionIds(form.elements.partition_ids.value.trim())
      : [];

    if (throughputInput === "" || !Number.isInteger(parsedThroughput) || parsedThroughput < 0) {
      throw new Error("throughput must be a non-negative integer");
    }

    if (assignmentStrategy === "manual" && ids.length === 0) {
      throw new Error("manual assignment requires at least one partition ID");
    }

    if (assignmentStrategy === "manual") {
      var manualValidationError = validateManualPartitionIds(ids);
      if (manualValidationError) {
        throw new Error(manualValidationError);
      }
    }

    return {
      metadata: {
        name: form.elements.name.value,
        owner: form.elements.owner.value,
        service_name_pattern: form.elements.service_name_pattern.value,
      },
      assignment: {
        name: form.elements.name.value,
        throughput_bytes: parsedThroughput,
        partition_ids: ids,
        require_dedicated_partition: partitionMode === "dedicated",
      },
      assignmentStrategy: assignmentStrategy,
      partitionMode: partitionMode,
    };
  }

  function validateDatasetAssignmentCapability(bundle) {
    return probePartitionCatalogSupport().then(function (catalogSupported) {
      if (!catalogSupported && bundle.assignmentStrategy === "auto") {
        throw new Error("auto-assignment requires partition catalog support");
      }
      if (!catalogSupported && (bundle.partitionMode === "shared" || bundle.partitionMode === "dedicated")) {
        throw new Error("shared/dedicated mode choices require partition catalog support");
      }
      return bundle;
    });
  }

  function saveDatasetAssignment(bundle) {
    return apiCall("UpdatePartitionAssignment", bundle.assignment);
  }

  function rollbackCreatedDataset(name, originalError) {
    return apiCall("DeleteDatasetMetadata", { name: name }).then(
      function () {
        throw originalError;
      },
      function (rollbackError) {
        throw new Error(
          originalError.message +
            ". Dataset was created, but rollback failed: " +
            rollbackError.message
        );
      }
    );
  }

  function initDatasets() {
    document.getElementById("btn-new-dataset").addEventListener("click", function () {
      showDatasetForm(null);
    });

    document.getElementById("btn-dataset-back").addEventListener("click", showDatasetList);
    document.getElementById("btn-dataset-cancel").addEventListener("click", showDatasetList);

    document
      .getElementById("form-dataset")
      .addEventListener("submit", function (e) {
        e.preventDefault();
        var form = this;
        var isEdit = form.dataset.editing === "true";
        var bundle;

        try {
          bundle = parseDatasetForm(form);
        } catch (err) {
          showToast("Error: " + err.message, "error");
          return;
        }

        validateDatasetAssignmentCapability(bundle)
          .then(function () {
            if (isEdit) {
              return apiCall("UpdateDatasetMetadata", bundle.metadata)
                .then(function () {
                  return saveDatasetAssignment(bundle);
                })
                .then(function () {
                  showToast("Dataset updated");
                  showDatasetList();
                  if (partitionCatalogSupport.status === "available") loadPartitions();
                });
            }

            return apiCall("CreateDatasetMetadata", bundle.metadata)
              .then(function () {
                return saveDatasetAssignment(bundle).catch(function (err) {
                  return rollbackCreatedDataset(bundle.metadata.name, err);
                });
              })
              .then(function () {
                showToast("Dataset created");
                showDatasetList();
                if (partitionCatalogSupport.status === "available") loadPartitions();
              });
          })
          .catch(function (err) {
            showToast("Error: " + err.message, "error");
            refreshRelatedViews();
          });
      });

    document
      .getElementById("form-dataset")
      .elements.assignment_strategy.addEventListener("change", updateDatasetFormState);
    document
      .getElementById("form-dataset")
      .elements.partition_mode.addEventListener("change", updateDatasetCapabilityNote);
  }

  // ---- Partitions ----

  function getPartitionOccupancyType(partition) {
    if (partition.dedicated) return "Dedicated";
    if (partition.shared) return "Shared";
    return "Empty";
  }

  function getPartitionDatasets(partition) {
    if (partition.dedicated && partition.dedicated.dataset) {
      return [partition.dedicated.dataset];
    }
    if (partition.shared && partition.shared.datasets) {
      return partition.shared.datasets.slice();
    }
    return [];
  }

  function partitionOccupancyBadgeHtml(partition) {
    var occupancyType = getPartitionOccupancyType(partition);
    var cssClass = "occupancy-empty";
    if (occupancyType === "Shared") cssClass = "occupancy-shared";
    if (occupancyType === "Dedicated") cssClass = "occupancy-dedicated";
    return (
      '<span class="occupancy-badge ' +
      cssClass +
      '">' +
      escapeHtml(occupancyType) +
      "</span>"
    );
  }

  function renderPartitionActions(partitionId) {
    var encodedId = encodeURIComponent(partitionId);
    return gridjs.html(
      '<div class="grid-actions">' +
      '<button class="btn btn-sm btn-danger" data-action="delete-partition" data-partition="' +
      encodedId +
      '">Delete</button>' +
      "</div>"
    );
  }

  function bindPartitionGridActions() {
    if (partitionGridActionsBound) return;

    var container = document.getElementById("partitions-grid");
    if (!container) return;

    container.addEventListener("click", function (e) {
      var target = e.target.closest("[data-action='delete-partition'][data-partition]");
      if (!target) return;

      var partitionId = decodeURIComponent(target.getAttribute("data-partition") || "");
      var partition = partitionById[partitionId];
      if (!partition) return;

      var datasets = getPartitionDatasets(partition);
      var occupancy = getPartitionOccupancyType(partition);
      var details = datasets.length ? " Current occupants: " + datasets.join(", ") + "." : "";

      showConfirm(
        'Delete partition "' + partitionId + '" from the catalog? Occupancy: ' + occupancy + "." + details,
        function () {
          apiCall("DeletePartition", { partition_id: partitionId })
            .then(function (resp) {
              showToast(resp.status || "Partition deleted");
              loadPartitions();
            })
            .catch(function (err) {
              showToast("Error: " + err.message, "error");
            });
        }
      );
    });

    partitionGridActionsBound = true;
  }

  function renderPartitionStats(partitions) {
    var target = document.getElementById("partitions-stats");
    if (!target) return;

    var total = partitions.length;
    var empty = 0;
    var shared = 0;
    var dedicated = 0;
    var totalMaxCapacity = 0;
    var totalProvisionedCapacity = 0;
    var i;

    for (i = 0; i < partitions.length; i++) {
      var partition = partitions[i];
      var occupancy = getPartitionOccupancyType(partition);
      if (occupancy === "Empty") empty++;
      if (occupancy === "Shared") shared++;
      if (occupancy === "Dedicated") dedicated++;
      totalMaxCapacity += toNumericValue(partition.maxCapacity);
      totalProvisionedCapacity += toNumericValue(partition.provisionedCapacity);
    }

    target.innerHTML =
      '<div class="stat-card"><span class="stat-card-label">Catalog Partitions</span><span class="stat-card-value">' +
      escapeHtml(String(total)) +
      '</span><span class="stat-card-subcopy">Total known partition entries</span></div>' +
      '<div class="stat-card"><span class="stat-card-label">Empty</span><span class="stat-card-value">' +
      escapeHtml(String(empty)) +
      '</span><span class="stat-card-subcopy">Available for future assignments</span></div>' +
      '<div class="stat-card"><span class="stat-card-label">Shared</span><span class="stat-card-value">' +
      escapeHtml(String(shared)) +
      '</span><span class="stat-card-subcopy">Partitions currently shared by datasets</span></div>' +
      '<div class="stat-card"><span class="stat-card-label">Dedicated</span><span class="stat-card-value">' +
      escapeHtml(String(dedicated)) +
      '</span><span class="stat-card-subcopy">Partitions reserved by one dataset</span></div>' +
      '<div class="stat-card"><span class="stat-card-label">Provisioned Capacity</span><span class="stat-card-value">' +
      escapeHtml(formatWithSeparators(totalProvisionedCapacity)) +
      '</span><span class="stat-card-subcopy">Of ' +
      escapeHtml(formatWithSeparators(totalMaxCapacity)) +
      " bytes total max capacity</span></div>";
  }

  function setPartitionsEmptyState(title, copy, visible) {
    var emptyState = document.getElementById("partitions-empty-state");
    if (!emptyState) return;

    emptyState.hidden = !visible;
    if (!visible) return;

    var titleNode = emptyState.querySelector("h3");
    var copyNode = document.getElementById("partitions-empty-copy");
    titleNode.textContent = title;
    copyNode.textContent = copy;
  }

  function renderPartitions(partitions) {
    if (typeof gridjs === "undefined") {
      showToast("gridjs is not available", "error");
      return;
    }

    var container = document.getElementById("partitions-grid");
    container.hidden = false;

    partitionById = {};
    var rows = [];
    var i;

    for (i = 0; i < partitions.length; i++) {
      var partition = partitions[i];
      var datasets = getPartitionDatasets(partition);
      var maxCapacity = toNumericValue(partition.maxCapacity);
      var provisionedCapacity = toNumericValue(partition.provisionedCapacity);
      var availableCapacity = Math.max(maxCapacity - provisionedCapacity, 0);

      partitionById[partition.partitionId] = partition;
      rows.push([
        partition.partitionId,
        toDisplayNumber(partition.maxCapacity),
        toDisplayNumber(partition.provisionedCapacity),
        toDisplayNumber(availableCapacity),
        gridjs.html(partitionOccupancyBadgeHtml(partition)),
        datasets.length ? datasets.join(", ") : "-",
        renderPartitionActions(partition.partitionId),
      ]);
    }

    renderPartitionStats(partitions);

    if (partitionsGrid) {
      partitionsGrid.updateConfig({ data: rows }).forceRender();
    } else {
      partitionsGrid = new gridjs.Grid({
        columns: [
          gridColumn("Partition ID", "Canonical Kafka partition identifier."),
          gridColumn("Max Capacity", "Configured capacity ceiling in bytes/sec."),
          gridColumn("Provisioned", "Capacity currently reserved by dataset assignments."),
          gridColumn("Available", "Unreserved capacity available for future assignments."),
          gridColumn("Occupancy", "Current catalog state for this partition."),
          gridColumn("Datasets", "Datasets currently assigned to this partition."),
          gridColumn("Actions", "Edit or delete this partition.", { sort: false }),
        ],
        data: rows,
        search: true,
        sort: true,
        pagination: {
          enabled: true,
          limit: 20,
        },
      });

      partitionsGrid.render(container);
      bindPartitionGridActions();
    }

    setPartitionsEmptyState(
      "No catalog partitions yet",
      "Create at least one partition before using shard auto-assignment or manual catalog validation.",
      partitions.length === 0
    );
  }

  function renderUnavailablePartitions(errorMessage) {
    var container = document.getElementById("partitions-grid");
    if (container) container.hidden = true;
    renderPartitionStats([]);
    setPartitionsEmptyState(
      "Partition catalog unavailable",
      errorMessage || "This manager build does not expose partition catalog APIs yet.",
      true
    );
  }

  function loadPartitions() {
    fetchPartitionMetadata()
      .then(function (partitions) {
        renderPartitions(partitions);
      })
      .catch(function (err) {
        renderUnavailablePartitions(err.message);
      });
  }

  function openCreatePartitionModal() {
    var form = document.getElementById("form-create-partition");
    form.reset();
    openModal("modal-create-partition");
  }

  function initPartitions() {
    document.getElementById("btn-refresh-partitions").addEventListener("click", function () {
      loadPartitions();
    });

    document.getElementById("btn-new-partition").addEventListener("click", function () {
      probePartitionCatalogSupport(true).then(function (catalogSupported) {
        if (!catalogSupported) {
          showToast("Partition catalog is unavailable on this manager build", "error");
          renderUnavailablePartitions(partitionCatalogSupport.lastError);
          return;
        }
        openCreatePartitionModal();
      });
    });

    document
      .getElementById("form-create-partition")
      .addEventListener("submit", function (e) {
        e.preventDefault();
        var form = this;
        var partitionId = form.elements.partition_id.value.trim();
        var maxCapacityInput = form.elements.max_capacity.value.trim();
        var parsedMaxCapacity = Number(maxCapacityInput);

        if (!isCanonicalNonNegativeIntegerString(partitionId)) {
          showToast("Error: partition ID must be a canonical non-negative integer", "error");
          return;
        }
        if (maxCapacityInput === "" || !Number.isInteger(parsedMaxCapacity) || parsedMaxCapacity <= 0) {
          showToast("Error: max capacity must be a positive integer", "error");
          return;
        }

        probePartitionCatalogSupport().then(function (catalogSupported) {
          if (!catalogSupported) {
            showToast("Error: partition catalog is unavailable on this manager build", "error");
            return;
          }

          apiCall("CreatePartition", {
            partition_id: partitionId,
            max_capacity: parsedMaxCapacity,
          })
            .then(function () {
              closeModal("modal-create-partition");
              showToast("Partition created");
              loadPartitions();
            })
            .catch(function (err) {
              showToast("Error: " + err.message, "error");
            });
        });
      });
  }

  // ---- Field Redactions ----

  function renderRedactionActions(redactionName) {
    var encodedName = encodeURIComponent(redactionName);
    return gridjs.html(
      '<div class="grid-actions">' +
      '<button class="btn btn-sm btn-danger" data-action="delete-redaction" data-redaction="' +
      encodedName +
      '">Delete</button>' +
      "</div>"
    );
  }

  function bindRedactionGridActions() {
    if (redactionGridActionsBound) return;

    var container = document.getElementById("redactions-grid");
    if (!container) return;

    container.addEventListener("click", function (e) {
      var target = e.target.closest("[data-action='delete-redaction'][data-redaction]");
      if (!target) return;

      var redactionName = decodeURIComponent(target.getAttribute("data-redaction") || "");
      var redaction = redactionByName[redactionName];
      if (!redaction) return;

      showConfirm('Delete redaction "' + redaction.name + '"?', function () {
        apiCall("DeleteFieldRedaction", { name: redaction.name })
          .then(function () {
            showToast("Redaction deleted");
            loadRedactions();
          })
          .catch(function (err) {
            showToast("Error: " + err.message, "error");
          });
      });
    });

    redactionGridActionsBound = true;
  }

  function loadRedactions() {
    apiCall("ListFieldRedactions", {})
      .then(function (data) {
        renderRedactions(data.redactedFields || []);
      })
      .catch(function (err) {
        showToast("Failed to load redactions: " + err.message, "error");
      });
  }

  function renderRedactions(redactions) {
    if (typeof gridjs === "undefined") {
      showToast("gridjs is not available", "error");
      return;
    }

    redactionByName = {};
    var rows = [];
    var i;

    for (i = 0; i < redactions.length; i++) {
      var redaction = redactions[i];
      redactionByName[redaction.name] = redaction;
      rows.push([
        redaction.name,
        redaction.fieldName || "",
        formatTime(redaction.startTimeEpochMs),
        formatTime(redaction.endTimeEpochMs),
        renderRedactionActions(redaction.name),
      ]);
    }

    if (redactionsGrid) {
      redactionsGrid.updateConfig({ data: rows }).forceRender();
      return;
    }

    redactionsGrid = new gridjs.Grid({
      columns: [
        gridColumn("Name", "Unique redaction rule identifier."),
        gridColumn("Field", "Field key redacted from search responses."),
        gridColumn("Start", "Start of redaction window in UTC."),
        gridColumn("End", "End of redaction window in UTC."),
        gridColumn("Actions", "Edit or delete this redaction rule.", { sort: false }),
      ],
      data: rows,
      sort: true,
      pagination: {
        enabled: true,
        limit: 20,
      },
    });

    redactionsGrid.render(document.getElementById("redactions-grid"));
    bindRedactionGridActions();
  }

  function initRedactions() {
    document.getElementById("btn-new-redaction").addEventListener("click", function () {
      document.getElementById("form-redaction").reset();
      openModal("modal-redaction");
    });

    document
      .getElementById("form-redaction")
      .addEventListener("submit", function (e) {
        e.preventDefault();
        var form = this;
        var body = {
          name: form.elements.name.value,
          field_name: form.elements.field_name.value,
          start_time_epoch_ms: parseInt(form.elements.start_time_epoch_ms.value, 10),
          end_time_epoch_ms: parseInt(form.elements.end_time_epoch_ms.value, 10),
        };

        apiCall("CreateFieldRedaction", body)
          .then(function () {
            closeModal("modal-redaction");
            showToast("Redaction created");
            loadRedactions();
          })
          .catch(function (err) {
            showToast("Error: " + err.message, "error");
          });
      });
  }

  // ---- Operations ----

  function initOperations() {
    document
      .getElementById("form-restore-replica")
      .addEventListener("submit", function (e) {
        e.preventDefault();
        var form = this;
        var result = document.getElementById("result-restore-replica");
        var body = {
          service_name: form.elements.service_name.value,
          start_time_epoch_ms: parseInt(form.elements.start_time_epoch_ms.value, 10),
          end_time_epoch_ms: parseInt(form.elements.end_time_epoch_ms.value, 10),
        };

        apiCall("RestoreReplica", body)
          .then(function (data) {
            result.textContent = JSON.stringify(data, null, 2);
            result.classList.add("visible");
            showToast("Restore initiated");
          })
          .catch(function (err) {
            result.textContent = "Error: " + err.message;
            result.classList.add("visible");
            showToast("Error: " + err.message, "error");
          });
      });

    document
      .getElementById("form-restore-replica-ids")
      .addEventListener("submit", function (e) {
        e.preventDefault();
        var form = this;
        var result = document.getElementById("result-restore-replica-ids");
        var raw = form.elements.ids_to_restore.value.trim();
        var ids = raw
          .split("\n")
          .map(function (item) {
            return item.trim();
          })
          .filter(function (item) {
            return item.length > 0;
          });

        apiCall("RestoreReplicaIds", { ids_to_restore: ids })
          .then(function (data) {
            result.textContent = JSON.stringify(data, null, 2);
            result.classList.add("visible");
            showToast("Restore by IDs initiated");
          })
          .catch(function (err) {
            result.textContent = "Error: " + err.message;
            result.classList.add("visible");
            showToast("Error: " + err.message, "error");
          });
      });

    document
      .getElementById("form-reset-partition")
      .addEventListener("submit", function (e) {
        e.preventDefault();
        var form = this;
        var result = document.getElementById("result-reset-partition");
        var body = {
          partition_id: form.elements.partition_id.value,
          dry_run: form.elements.dry_run.checked,
        };

        apiCall("ResetPartitionData", body)
          .then(function (data) {
            result.textContent = JSON.stringify(data, null, 2);
            result.classList.add("visible");
            showToast("Partition reset " + (body.dry_run ? "(dry run)" : "complete"));
          })
          .catch(function (err) {
            result.textContent = "Error: " + err.message;
            result.classList.add("visible");
            showToast("Error: " + err.message, "error");
          });
      });
  }

  // ---- Modal close handlers ----

  function initModals() {
    var closeBtns = document.querySelectorAll(".modal-close, [data-dismiss='modal']");
    var overlays = document.querySelectorAll(".modal-overlay");
    var i;

    for (i = 0; i < closeBtns.length; i++) {
      closeBtns[i].addEventListener("click", function () {
        closeAllModals();
      });
    }

    for (i = 0; i < overlays.length; i++) {
      overlays[i].addEventListener("click", function (e) {
        if (e.target === this) closeAllModals();
      });
    }

    document.addEventListener("keydown", function (e) {
      if (e.key === "Escape") closeAllModals();
    });
  }

  // ---- Init ----

  function init() {
    initTabs();
    initModals();
    initConfirmDialog();
    initDangerConfirmDialog();
    initDatasets();
    initPartitions();
    initRedactions();
    initOperations();
    probePartitionCatalogSupport();
    loadDatasets();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
