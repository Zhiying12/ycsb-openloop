#!/bin/bash

readonly DB=$1
readonly TEST_SETTING=$2
readonly OUTPUT_PATH=scripts/max-throughput-latency/${TEST_SETTING}
readonly TMP_OUTPUT_PATH=${OUTPUT_PATH}/tmp
mkdir -p $TMP_OUTPUT_PATH

readonly WARMUP_DUARTION=10
readonly RUN_DURATION=70
readonly RECORD_COUNT=2000000
readonly OP_COUNT_PER_CLIENT=3000000
readonly CLIENTS=(64 100 150 200)

./bin/ycsb load $DB -P workloads/workloada \
  -p recordcount=$RECORD_COUNT \
  -p fieldcount=5 \
  -threads 8 -s

for i in ${!CLIENTS[@]}; do
  sleep 10
  client=${CLIENTS[$i]}
  target_op_count=$((client * OP_COUNT_PER_CLIENT))
  log_path=${TMP_OUTPUT_PATH}/${client}_client_wa.dat
  ./bin/ycsb run $DB -P workloads/workloada \
    -p recordcount=$RECORD_COUNT \
    -p operationcount=$target_op_count \
    -p maxexecutiontime=$RUN_DURATION \
    -p fieldcount=5 \
    -p writeallfields=true \
    -p combineop=true \
    -p measurement.interval=both \
    -p warmup=10 \
    -p exportercdf=true \
    -threads $client -s | tee $log_path

  mv OVERALL-latency-cdf.dat ${OUTPUT_PATH}/${client}-clients.dat
  mv OVERALL-distribution.dat ${OUTPUT_PATH}/${client}-clients-distribution.dat
  mv OVERALL-all-latency.dat ${OUTPUT_PATH}/${client}-clients-all-latency.dat
  
  dat_file=${OUTPUT_PATH}/result.dat
  if [ ! -f $dat_file ]; then
    echo "Clients Throughput Average 99th Intended-Average Intended-99th" >$dat_file
  fi

  ops=`awk 'NR==2' ${log_path} | awk -F", " '{print $3}'`
  avg=`awk 'NR==4' ${log_path} | awk -F", " '{print $3}'`
  tail_latency=`awk 'NR==8' ${log_path} | awk -F", " '{print $3}'`
  intended_avg=`awk 'NR==11' ${log_path} | awk -F", " '{print $3}'`
  intended_tail=`awk 'NR==15' ${log_path} | awk -F", " '{print $3}'`
  echo "${client} ${ops} ${avg} ${tail_latency} ${intended_avg} ${intended_tail}" >>$dat_file
done
