(function () {
  "use strict";

  var API_BASE = "/slack.proto.astra.ManagerApiService";

  // ---- API helper ----

  function apiCall(method, body) {
    return fetch(API_BASE + "/" + method, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body || {}),
    }).then(function (res) {
      return res.json().then(function (data) {
        if (!res.ok) {
          throw new Error(data.message || data.error || JSON.stringify(data));
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

  function initTabs() {
    var tabs = document.querySelectorAll(".tab");
    for (var i = 0; i < tabs.length; i++) {
      tabs[i].addEventListener("click", function () {
        var target = this.getAttribute("data-tab");
        var allTabs = document.querySelectorAll(".tab");
        var allContent = document.querySelectorAll(".tab-content");
        for (var j = 0; j < allTabs.length; j++) {
          allTabs[j].classList.remove("active");
        }
        for (var k = 0; k < allContent.length; k++) {
          allContent[k].classList.remove("active");
        }
        this.classList.add("active");
        document.getElementById(target).classList.add("active");

        if (target === "datasets") loadDatasets();
        if (target === "redactions") loadRedactions();
      });
    }
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
    setTimeout(function () { input.focus(); }, 100);
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

  // ---- Datasets ----

  var datasetsGrid = null;
  var datasetByName = {};
  var datasetGridActionsBound = false;

  function loadDatasets() {
    apiCall("ListDatasetMetadata", {})
      .then(function (data) {
        var datasets = data.datasetMetadata || [];
        renderDatasets(datasets);
      })
      .catch(function (err) {
        showToast("Failed to load datasets: " + err.message, "error");
      });
  }

  function getPartitionCount(partitionConfigs) {
    var partCount = 0;
    for (var i = 0; i < partitionConfigs.length; i++) {
      partCount += (partitionConfigs[i].partitions || []).length;
    }
    return partCount;
  }

  function formatPartitionConfigs(partitionConfigs) {
    if (!partitionConfigs || partitionConfigs.length === 0) return "-";

    var details = [];
    for (var i = 0; i < partitionConfigs.length; i++) {
      var pc = partitionConfigs[i];
      var ids = (pc.partitions || []).join(", ");
      details.push(
        "Start: " + formatTime(pc.startTimeEpochMs) +
        " | End: " + formatTime(pc.endTimeEpochMs) +
        " | IDs: [" + ids + "]"
      );
    }
    return details.join(" || ");
  }

  function renderDatasetActions(datasetName) {
    var encodedName = encodeURIComponent(datasetName);
    return gridjs.html(
      '<div class="grid-actions">' +
      '<button class="btn btn-sm" data-action="edit" data-dataset="' + encodedName + '">Edit</button>' +
      '<button class="btn btn-sm" data-action="partitions" data-dataset="' + encodedName + '">Capacity</button>' +
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

      if (action === "partitions") {
        openPartitionModal(dataset);
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
                loadDatasets();
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
    for (var i = 0; i < datasets.length; i++) {
      var ds = datasets[i];
      var partitions = ds.partitionConfigs || [];
      datasetByName[ds.name] = ds;

      rows.push([
        ds.name,
        ds.owner || "",
        ds.serviceNamePattern || "",
        ds.throughputBytes || 0,
        getPartitionCount(partitions),
        formatPartitionConfigs(partitions),
        renderDatasetActions(ds.name),
      ]);
    }

    if (datasetsGrid) {
      datasetsGrid.updateConfig({ data: rows }).forceRender();
      return;
    }

    datasetsGrid = new gridjs.Grid({
      columns: [
        "Name",
        "Owner",
        "Service Pattern",
        "Throughput (bytes)",
        "Partition Count",
        "Partition Details",
        { name: "Actions", sort: false },
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
      title.textContent = "Edit Dataset";
      form.elements.name.value = dataset.name;
      form.elements.name.readOnly = true;
      form.elements.owner.value = dataset.owner || "";
      form.elements.service_name_pattern.value = dataset.serviceNamePattern || "";
      form.dataset.editing = "true";
    } else {
      title.textContent = "New Dataset";
      form.elements.name.readOnly = false;
      form.dataset.editing = "false";
    }
  }

  function openPartitionModal(dataset) {
    var form = document.getElementById("form-partition");
    form.reset();
    form.elements.name.value = dataset.name;
    form.elements.throughput_bytes.value = dataset.throughputBytes == null ? 0 : dataset.throughputBytes;
    form.elements.partition_ids.value = "";
    openModal("modal-partition");
  }

  function initDatasets() {
    document.getElementById("btn-new-dataset").addEventListener("click", function () {
      showDatasetForm(null);
    });

    document.getElementById("btn-dataset-back").addEventListener("click", showDatasetList);
    document.getElementById("btn-dataset-cancel").addEventListener("click", showDatasetList);

    document.getElementById("form-dataset").addEventListener("submit", function (e) {
      e.preventDefault();
      var form = this;
      var isEdit = form.dataset.editing === "true";
      var method = isEdit ? "UpdateDatasetMetadata" : "CreateDatasetMetadata";
      var body = {
        name: form.elements.name.value,
        owner: form.elements.owner.value,
        service_name_pattern: form.elements.service_name_pattern.value,
      };

      apiCall(method, body)
        .then(function () {
          showToast(isEdit ? "Dataset updated" : "Dataset created");
          showDatasetList();
        })
        .catch(function (err) {
          showToast("Error: " + err.message, "error");
        });
    });

    document.getElementById("form-partition").addEventListener("submit", function (e) {
      e.preventDefault();
      var form = this;
      var ids = form.elements.partition_ids.value.trim();
      var throughputInput = form.elements.throughput_bytes.value.trim();
      var parsedThroughput = Number(throughputInput);
      if (throughputInput === "" || !Number.isInteger(parsedThroughput) || parsedThroughput < 0) {
        showToast("Error: throughput must be a non-negative integer", "error");
        return;
      }
      var body = {
        name: form.elements.name.value,
        throughput_bytes: parsedThroughput,
        partition_ids: ids
          ? ids
              .split(",")
              .map(function (s) { return s.trim(); })
              .filter(function (s) { return s.length > 0; })
          : [],
      };

      apiCall("UpdatePartitionAssignment", body)
        .then(function (resp) {
          closeModal("modal-partition");
          var assigned = (resp.assignedPartitionIds || []).join(", ");
          showToast("Partitions assigned: " + (assigned || "(none)"));
          loadDatasets();
        })
        .catch(function (err) {
          showToast("Error: " + err.message, "error");
        });
    });
  }

  // ---- Field Redactions ----
  var redactionsGrid = null;
  var redactionByName = {};
  var redactionGridActionsBound = false;

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

      showConfirm(
        'Delete redaction "' + redaction.name + '"?',
        function () {
          apiCall("DeleteFieldRedaction", { name: redaction.name })
            .then(function () {
              showToast("Redaction deleted");
              loadRedactions();
            })
            .catch(function (err) {
              showToast("Error: " + err.message, "error");
            });
        }
      );
    });

    redactionGridActionsBound = true;
  }

  function loadRedactions() {
    apiCall("ListFieldRedactions", {})
      .then(function (data) {
        var redactions = data.redactedFields || [];
        renderRedactions(redactions);
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
    for (var i = 0; i < redactions.length; i++) {
      var r = redactions[i];
      redactionByName[r.name] = r;
      rows.push([
        r.name,
        r.fieldName || "",
        formatTime(r.startTimeEpochMs),
        formatTime(r.endTimeEpochMs),
        renderRedactionActions(r.name),
      ]);
    }

    if (redactionsGrid) {
      redactionsGrid.updateConfig({ data: rows }).forceRender();
      return;
    }

    redactionsGrid = new gridjs.Grid({
      columns: [
        "Name",
        "Field Name",
        "Start Time",
        "End Time",
        { name: "Actions", sort: false },
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

    document.getElementById("form-redaction").addEventListener("submit", function (e) {
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
          .map(function (s) { return s.trim(); })
          .filter(function (s) { return s.length > 0; });

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

  // ---- Utilities ----


  var MAX_TIME = "9223372036854775807"; // Long.MAX_VALUE as string

  function formatTime(epochMs) {
    if (!epochMs || epochMs === "0") return "-";
    if (String(epochMs) === MAX_TIME) return "MAX";
    try {
      return new Date(Number(epochMs)).toISOString().replace("T", " ").replace(/\.000Z$/, " UTC");
    } catch (e) {
      return String(epochMs);
    }
  }

  // ---- Modal close handlers ----

  function initModals() {
    // Close buttons
    var closeBtns = document.querySelectorAll(".modal-close, [data-dismiss='modal']");
    for (var i = 0; i < closeBtns.length; i++) {
      closeBtns[i].addEventListener("click", function () {
        closeAllModals();
      });
    }
    // Click outside modal
    var overlays = document.querySelectorAll(".modal-overlay");
    for (var j = 0; j < overlays.length; j++) {
      overlays[j].addEventListener("click", function (e) {
        if (e.target === this) closeAllModals();
      });
    }
    // Escape key
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
    initRedactions();
    initOperations();
    loadDatasets();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
