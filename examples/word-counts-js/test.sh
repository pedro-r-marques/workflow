#!/bin/bash

BASEDIR="./examples/word-counts"
DIR="./examples/word-counts-js"
AMQP_SERVER=${AMQP_SERVER-"amqp://guest:guest@localhost:5672/"}
export AMQP_SERVER

WPIDS=()

./workflow-manager -db sqlite3:wm.db -config "${BASEDIR}/config.yaml" &
WM_PID=$!

pyworkers=(
    book_split.py
    store_title.py
)

for w in "${pyworkers[@]}"; do
    python "${BASEDIR}/$w" &
    WPIDS+=($!)
done

jsworkers=(
    segment_word_counts.js
    sum_splits.js
)

for w in "${jsworkers[@]}"; do
    node "${DIR}/build/$w" &
    WPIDS+=($!)
done


function cleanup() {
    for pid in "${WPIDS[@]}"; do
        kill "${pid}"
    done
    kill "${WM_PID}"
}

trap cleanup EXIT INT

function element_in_list() {
    local xvalue="$1"
    shift
    local xarr=("$@")
    for x in "${xarr[@]}"; do
        xunquote=$(echo "${x}" | sed -e 's/^"//' -e 's/"$//')
        if [ "${xunquote}" = "${xvalue}" ]; then
            return 0
        fi
    done
    return 1
}

# ensure workflow-manager is up
for i in $(seq 1 10); do
    curl -s "http://localhost:8080/api/workflows"
    if [ "$?" -eq 0 ]; then
        break
    fi
    sleep 1
done

# wait for the worker queues to be registered
for i in $(seq 1 10); do
    queue_names=($(curl -s http://guest:guest@localhost:15672/api/queues | jq ".[] | .name"))
    if [ ${#queue_names[@]} -ge 5 ]; then
        break
    fi
    sleep 1
done

# create a new job
params=(
    --header "Content-Type: application/json"
    --request POST
    --data '{"filename":"examples/word-counts/Alice_in_Wonderland.txt"}'
)

CREATE=$(curl -s "${params[@]}" http://localhost:8080/api/workflow/book-word-counts)
JOB_ID=$(echo "${CREATE}" | jq .id | sed -e 's/^"//' -e 's/"$//')

# wait until it is complete
for i in $(seq 1 10); do
    job_status=$(curl -s -f "http://localhost:8080/api/job/${JOB_ID}")
    if [ "$?" -ne 0 ]; then
        break
    fi
    completed=($(echo "${job_status}" | jq ".completed | .[] | .name"))
    element_in_list "__end__" "${completed[@]}"
    if [ "$?" -eq 0 ]; then
        echo "[OK]"
        exit 0
    fi
    sleep 1
done

echo "FAIL"
exit 1
