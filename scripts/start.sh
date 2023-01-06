#!/usr/bin/env bash

for id in 0 1 2; do
    "$(dirname "$(dirname $0)")"/clusters/node$id/start_node_$id.sh & sleep 1
done

trap 'kill $(jobs -p)' EXIT
wait < <(jobs -p)
