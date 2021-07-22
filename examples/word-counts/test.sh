#!/bin/bash


DIR="./examples/word-counts"
AMQP_SERVER=${AMQP_SERVER-"amqp://guest:guest@localhost:5672/"}
export AMQP_SERVER

WPIDS=()

./workflow-manager --config "${DIR}/config.yaml" &
WM_PID=$!

workers=(
    book_split.py
    segment_word_counts.py
    store_title.py
    sum_splits.py
)

for w in "${workers[@]}"; do
    python "${DIR}/$w" &
    WPIDS+=($!)
done

function cleanup() {
    for pid in "${WPIDS[@]}"; do
        kill "${pid}"
    done
    kill "${WM_PID}"
    exit
}

trap cleanup EXIT INT

params=(
    --header "Content-Type: application/json"
    --request POST
    --data '{"filename":"examples/word-counts/Alice_in_Wonderland.txt"}'
)

for i in $(seq 1 10); do
    curl -q "http://localhost:8080/api/workflows"
    if [ $? == 0 ]; then
        break
    fi
    sleep 1
done

# TODO: pool rabbitmq for all queues to be registered.
sleep 1

curl "${params[@]}" http://localhost:8080/api/workflow/book-word-counts

for i in $(seq 1 10); do
    echo "${i}"
    sleep 100
done


