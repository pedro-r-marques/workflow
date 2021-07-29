function isJobDone(response) {
    if (!("completed" in response)) {
        return false;
    }
    let exists = false;
    response["completed"].forEach(entry => {
        if (entry["name"] == "__end__") {
            exists = true;
        }
    });
    return exists;
}

function showJobStatus(response) {
    let idElement = document.querySelector("#job-id");
    idElement.innerHTML = response["uuid"];

    let statusElement = document.querySelector("#job-status");
    let completed = response["completed"];

    if (isJobDone(response)) {
        statusElement.innerHTML = "Done";
        let runTable = document.querySelector("#table-running");
        runTable.style.visibility = "hidden";
    } else {
        statusElement.innerHTML = "Running";
    }

    let runListElement = document.querySelector("#job-run-list");
    let nRunList = runListElement.cloneNode(false);
    let runEntries = [];
    if ("running" in response && response["running"] !== null) {
        runEntries = response["running"];
    }
    runEntries.forEach(entry => {
        let tr = document.createElement("tr");
        nRunList.appendChild(tr);
        for (let field of ["name", "startTime"]) {
            let td = document.createElement("td");
            tr.appendChild(td);
            if (field in entry) {
                td.innerHTML = entry[field];
            }
        }
        if ("tasks" in entry) {
            let tr = document.createElement("tr");
            nRunList.appendChild(tr);
            let td = tr.appendChild(document.createElement("td"));
            td.setAttribute("colspan", "2");
            td.classList.add("nested-table-parent");

            let tbl = td.appendChild(document.createElement("table"));

            tbl.classList.add("table");
            tbl.classList.add("table-hover");
            tbl.classList.add("table-borderless");

            tbl.appendChild(document.createElement("thead"))
                .appendChild(document.createElement("tr"))
                .appendChild(document.createElement("td"))
                .innerText = "Tasks";

            let tbody = tbl.appendChild(document.createElement("tbody"));
            entry["tasks"].forEach(task => {
                let tr = tbody.appendChild(document.createElement("tr"));
                tr.addEventListener("click", function (event) {
                    window.location.href = "job.html?id=" + task;
                });
                tr.appendChild(document.createElement("td"))
                    .innerHTML = task;
            });
        }
    });
    runListElement.parentNode.replaceChild(nRunList, runListElement);

    let doneListElement = document.querySelector("#job-done-list");
    let nDoneList = doneListElement.cloneNode(false);
    let doneEntries = [];
    if ("completed" in response && response["completed"] !== null) {
        doneEntries = response["completed"];
    }
    doneEntries.forEach(entry => {
        let tr = nDoneList.appendChild(document.createElement("tr"));
        for (let field of ["name", "startTime", "elapsed"]) {
            let td = tr.appendChild(document.createElement("td"));
            if (!(field in entry)) {
                continue;
            }
            if (field == "elapsed") {
                let v = entry[field] / 1000000
                td.innerHTML = v.toFixed(2).toString() + " ms";
            } else {
                td.innerHTML = entry[field];
            }
        }

        let td = nDoneList.appendChild(document.createElement("tr"))
            .appendChild(document.createElement("td"));
        td.setAttribute("colspan", "3");
        td.classList.add("json-cell");
        td.innerHTML = JSON.stringify(entry["message"], undefined, 4);

    });
    doneListElement.parentNode.replaceChild(nDoneList, doneListElement);
}

function displayJob() {
    const urlParams = new URLSearchParams(window.location.search);
    let jobId = urlParams.get("id");
    fetch('/api/job/' + jobId)
        .then(response => response.json())
        .then(function (response) {
            showJobStatus(response);
        });
}

function initialize() {
    displayJob();
}

document.addEventListener("DOMContentLoaded", () => {
    initialize();
})