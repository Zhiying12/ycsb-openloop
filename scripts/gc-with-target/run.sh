#!/bin/bash

readonly DB=$1
readonly TEST_SETTING=$6
readonly CLIENT_COUNT=$2
readonly RECORD_COUNT=$3
readonly TARGET_TPUT=$4
readonly OUTPUT_PATH=scripts/gc-with-target/${TEST_SETTING}
mkdir -p $OUTPUT_PATH

readonly RUN_DURATION=$5
readonly OPERATION_COUNT=$((TARGET_TPUT * RUN_DURATION + TARGET_TPUT))

./bin/ycsb load $DB -P workloads/workloada \
  -p recordcount=$RECORD_COUNT \
  -p fieldcount=5 \
  -target $TARGET_TPUT \
  -threads 64 -s

sleep 10

./bin/ycsb run $DB -P workloads/workloada \
    -p recordcount=$RECORD_COUNT \
    -p operationcount=$OPERATION_COUNT \
    -p maxexecutiontime=$RUN_DURATION \
    -p fieldcount=5 \
    -p writeallfields=true \
    -p combineop=true \
    -target $TARGET_TPUT \
    -threads $CLIENT_COUNT -s \
    1>${OUTPUT_PATH}/stat.log 2>${OUTPUT_PATH}/status.log

# mv Intended-OVERALL-latency-cdf.dat ${OUTPUT_PATH}/intended-latency.dat
# mv OVERALL-latency-cdf.dat ${OUTPUT_PATH}/latency.dat
cat ${OUTPUT_PATH}/status.log

echo "Duration Throughput Avg-latency 99th 99.9th Avg-Intended-latency 99th-intended 99.9th-intended" >${OUTPUT_PATH}/status.dat
sed '1,2d;$d' ${OUTPUT_PATH}/status.log \
    | awk '{printf $3" "$7" "; for(i=8; i<=NF; i++) {if(match($i, /^(Avg=|99=|99.9=)/)){printf $i" "}} print ""}' \
    | sed -E "s/,?\s.{2,4}=/ /g" \
    | sed -E "s/,//" >> ${OUTPUT_PATH}/status.dat
