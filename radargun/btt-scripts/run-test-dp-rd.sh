#!/bin/bash

WORKING_DIR=`cd $(dirname $0); pwd`

echo "loading environment..."
. ${WORKING_DIR}/environment.sh

NR_NODES_TO_USE="40"
EST_DURATION="120"

ISPN_DEFAULT="-stats -write-skew -versioned -clustering-mode d -extended-stats -num-owner 1 -dp-max-keys 1000 -dp-bf-fp 0.01"
#RADARGUN CONFIG
#BENC_DEFAULT="-nr-thread 2 -nr-keys 1000000 -simul-time 60000 -distributed -write-tx-percentage 50 -write-tx-workload 10,20:10,20 -read-tx-workload 20,40"
#TPC-C CONFIG
BENC_DEFAULT="-population-batch-level 25 -population-threads 4 -nr-thread 2 -simul-time 60000 -distributed -nr-warehouse 40 -payment-weight 0 -order-status-weight 100 -same-warehouse-access"

echo "============ INIT BENCHMARKING ==============="

clean_master
kill_java ${CLUSTER}
clean_slaves ${CLUSTER}

echo "============ STARTING BENCHMARKING ==============="

#lp => locality probability= 0 15 50 75 100
#wrtPx => write percentage== 0 10
#rdg => replication degree== 1 2 3

${JGRP_GEN} -tcp

for lp in 0 50 100; do
for protocol in -c50-data-placement; do
#for l1 in -l1-rehash none; do
#for wrtPx in 0 10; do
#for rdg in 1 2 3; do
#for keys in 1000 8000 16000; do
#for bfFp in 0.01 0.10; do
${ISPN_GEN} ${ISPN_DEFAULT} $protocol
${BENC_GEN} ${BENC_DEFAULT} -std-dev $lp
run_test ${NR_NODES_TO_USE} "std-dev_${lp}-p_${protocol}" ${EST_DURATION} ${CLUSTER}
#done
#done
#done
done
done

echo "============ FINISHED BENCHMARKING ==============="

exit 0
