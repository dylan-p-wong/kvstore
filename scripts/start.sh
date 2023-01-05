#!/usr/bin/env bash

for id in 0 1 2; do
    "`dirname \`pwd\``"/clusters/node$id/start_node_$id.sh & sleep 2
done

trap 'kill $(jobs -p)' EXIT
wait < <(jobs -p)
