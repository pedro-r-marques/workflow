function makeJobHRef(entry) {
    let href = "job.html?id=" + encodeURIComponent(entry);
    return href;
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
            response["jobs"].forEach(entry => {
                let tr = document.createElement("tr");
                nlist.appendChild(tr);
                tr.addEventListener("click", function (event) {
                    window.location.href = makeJobHRef(entry);
                })
                let td = document.createElement("td");
                tr.appendChild(td);
                td.innerHTML = entry;
            });
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