function makeJobHRef(entry) {
    let href = "job.html?id=" + encodeURIComponent(entry);
    return href;
}

function addJobRowWithStatus(nlist, jobID, status) {
    let tr = document.createElement("tr");
    nlist.appendChild(tr);
    tr.addEventListener("click", function (event) {
        window.location.href = makeJobHRef(jobID);
    })

    tr.appendChild(document.createElement("td")).innerHTML = jobID;
    tr.appendChild(document.createElement("td")).innerHTML = status;
}

function displayJobs() {
    const urlParams = new URLSearchParams(window.location.search);
    let url = urlParams.get("name");
    if (urlParams.get("vhost")) {
        url += "?vhost=" + encodeURIComponent(urlParams.get("vhost"));
    }
    fetch('/api/workflow/' + url)
        .then(response => response.json())
        .then(function (response) {
            let current = document.querySelector('#job-list');
            let nlist = current.cloneNode(false);
            if ("running" in response && response["running"] !== null) {
                response["running"].forEach(entry => {
                    addJobRowWithStatus(nlist, entry, "running")
                });
            }
            if ("completed" in response && response["completed"] !== null) {
                response["completed"].forEach(entry => {
                    addJobRowWithStatus(nlist, entry, "completed")
                });
            }
            let parent = current.parentNode;
            parent.replaceChild(nlist, current);
        });
}

function initialize() {
    displayJobs();
}

document.addEventListener("DOMContentLoaded", () => {
    initialize();
})