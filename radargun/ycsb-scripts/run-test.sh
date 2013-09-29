#!/bin/bash

WORKING_DIR=`cd $(dirname $0); pwd`

echo "loading environment..."
. ${WORKING_DIR}/environment.sh

READONLY=$1
REMOTE=$2
COUNT=$3
TO=$4
NR_NODES_TO_USE=`wc -l /home/ndiegues/machines | awk '{print $1}'`
EST_DURATION="1"

#ISPN_DEFAULT="-stats -write-skew -versioned -clustering-mode d -extended-stats -num-owner 1"
#RADARGUN CONFIG
#BENC_DEFAULT="-nr-thread 2 -nr-keys 1000000 -simul-time 60000 -distributed -write-tx-percentage 50 -write-tx-workload 10,20:10,20 -read-tx-workload 20,40"
#TPC-C CONFIG
BENC_DEFAULT="-distributed -c $NR_NODES_TO_USE -l 1 -t 30000 -ro $READONLY -rem $REMOTE -count $COUNT -rc $COUNT -to $TO"

echo "============ INIT BENCHMARKING ==============="

clean_master
# kill_java ${CLUSTER}
# clean_slaves ${CLUSTER}

echo "============ STARTING BENCHMARKING ==============="

#lp => locality probability= 0 15 50 75 100
#wrtPx => write percentage== 0 10
#rdg => replication degree== 1 2 3

${JGRP_GEN} -sequencer -toa -tcp

for owner in 1; do
#for l1 in -l1-rehash none; do
#for wrtPx in 0 10; do
#for rdg in 1 2 3; do
#for keys in 1000 8000 16000; do
#for bfFp in 0.01 0.10; do

#${ISPN_GEN} ${ISPN_DEFAULT} -num-owner ${owner}
echo "=== Before benchmark generation ==="
${BENC_GEN} ${BENC_DEFAULT}
echo "==== Generated the benchmark configuration ===="
run_test ${NR_NODES_TO_USE} "results2" ${EST_DURATION} ${CLUSTER}
killall -9 java
done
#done
#done

echo "============ FINISHED BENCHMARKING ==============="

exit 0
