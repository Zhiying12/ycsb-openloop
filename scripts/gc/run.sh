#!/bin/bash

readonly DB=$1
readonly TEST_SETTING=$4
readonly CLIENT_COUNT=$2
readonly RECORD_COUNT=$3
readonly OUTPUT_PATH=scripts/gc/${TEST_SETTING}
mkdir -p $OUTPUT_PATH

readonly WARMUP_DUARTION=10
readonly RUN_DURATION=610
readonly OPERATION_COUNT=$((CLIENT_COUNT * RECORD_COUNT))

./bin/ycsb load $DB -P workloads/workloada \
  -p recordcount=$RECORD_COUNT \
  -p fieldcount=5 \
  -threads $CLIENT_COUNT -s

sleep 20

./bin/ycsb run $DB -P workloads/workloada \
    -p recordcount=$RECORD_COUNT \
    -p operationcount=$OPERATION_COUNT \
    -p maxexecutiontime=$RUN_DURATION \
    -p fieldcount=5 \
    -p writeallfields=true \
    -p combineop=true \
    -p warmup=10 \
    -p exportercdf=true \
    -threads $CLIENT_COUNT -s \
    1>${OUTPUT_PATH}/stat.log 2>${OUTPUT_PATH}/status.log

echo "Duration Throughput Avg-latency 99th 99.9th" >${OUTPUT_PATH}/status.dat
sed '1,3d;$d' ${OUTPUT_PATH}/status.log \
    | awk '{printf $3" "$7" "; for(i=8; i<=NF; i++) {if(match($i, /^(Avg=|99=|99.9=)/)){printf $i" "}} print ""}' \
    | gsed -E "s/,?\s.{2,4}=/ /g" \
    | gsed -E "s/,//" >> ${OUTPUT_PATH}/status.dat

