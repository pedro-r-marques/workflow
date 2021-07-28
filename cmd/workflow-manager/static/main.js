function makeWorkflowHRef(entry) {
    let href = "workflow.html?name=" + encodeURIComponent(entry["name"]);
    if ("vhost" in entry) {
        href += "&vhost=" + encodeURIComponent(entry["vhost"]);
    }
    return href;
}


function displayWorkflows() {
    fetch('/api/workflows')
        .then(response => response.json())
        .then(function (response) {
            let current = document.querySelector('#workflow-list');
            let nlist = current.cloneNode(false);
            response.forEach(entry => {
                let tr = document.createElement("tr");
                nlist.appendChild(tr);
                tr.addEventListener("click", function (event) {
                    window.location.href = makeWorkflowHRef(entry);
                })
                for (let field of ["vhost", "name", "job_count"]) {
                    let td = document.createElement("td");
                    tr.appendChild(td);
                    if (field in entry) {
                        td.innerHTML = entry[field];
                    }
                }
            });
            let parent = current.parentNode;
            parent.replaceChild(nlist, current);
        });
}

function initialize() {
    displayWorkflows();
}

document.addEventListener("DOMContentLoaded", () => {
    initialize();
})