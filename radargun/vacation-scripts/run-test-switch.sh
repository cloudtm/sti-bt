#!/bin/bash

WORKING_DIR=`cd $(dirname $0); pwd`

echo "loading environment..."
. ${WORKING_DIR}/environment.sh

NR_NODES_TO_USE="10"
EST_DURATION="5"

clean_master
kill_java ${CLUSTER}
clean_slaves ${CLUSTER}

ISPN_DEFAULT="-stats -write-skew -versioned -to-1pc"
HIGH_CONFLICT="-nr-warehouse 1 -nr-thread 1 -arrival-rate 10000"
LOW_CONFLICT="-nr-warehouse 10 -same-warehouse-access -nr-thread 1 -arrival-rate 10000"
BIG_WS="-payment-weight 5 -order-status-weight 50 -nr-items-inter 15,30"
SMALL_WS="-payment-weight 45 -order-status-weight 50"

echo "============ STARTING BENCHMARKING ==============="

#High conflict with big write set
${JGRP_GEN}
${BENC_GEN} ${HIGH_CONFLICT} ${BIG_WS}
${ISPN_GEN} ${ISPN_DEFAULT} ${ISPN_HC}
run_test ${NR_NODES_TO_USE} "HI-BIG-RC-2PC-run1" ${EST_DURATION} ${CLUSTER}

${BENC_GEN} ${HIGH_CONFLICT} ${BIG_WS} -passive-replication
${ISPN_GEN} ${ISPN_DEFAULT} ${ISPN_HC} -pb-protocol
run_test ${NR_NODES_TO_USE} "HI-BIG-RC-PR-run2" ${EST_DURATION} ${CLUSTER}

${JGRP_GEN} -sequencer
${BENC_GEN} ${HIGH_CONFLICT} ${BIG_WS}
${ISPN_GEN} ${ISPN_DEFAULT} ${ISPN_HC} -to-protocol
run_test ${NR_NODES_TO_USE} "HI-BIG-RC-TO-run3" ${EST_DURATION} ${CLUSTER}

Low conflict with big write set
${JGRP_GEN}
${BENC_GEN} ${LOW_CONFLICT} ${BIG_WS}
${ISPN_GEN} ${ISPN_DEFAULT} ${ISPN_LC}
run_test ${NR_NODES_TO_USE} "LOW-BIG-RC-2PC-run4" ${EST_DURATION} ${CLUSTER}

${BENC_GEN} ${LOW_CONFLICT} ${BIG_WS} -passive-replication
${ISPN_GEN} ${ISPN_DEFAULT} ${ISPN_LC} -pb-protocol
run_test ${NR_NODES_TO_USE} "LOW-BIG-RC-PR-run5" ${EST_DURATION} ${CLUSTER}

${JGRP_GEN} -sequencer
${BENC_GEN} ${LOW_CONFLICT} ${BIG_WS}
${ISPN_GEN} ${ISPN_DEFAULT} ${ISPN_LC} -to-protocol
run_test ${NR_NODES_TO_USE} "LOW-BIG-RC-TO-run6" ${EST_DURATION} ${CLUSTER}

#High conflict with small write set
${JGRP_GEN}
${BENC_GEN} ${HIGH_CONFLICT} ${SMALL_WS}
${ISPN_GEN} ${ISPN_DEFAULT} ${ISPN_HC}
run_test ${NR_NODES_TO_USE} "HI-SMALL-RC-2PC-run7" ${EST_DURATION} ${CLUSTER}

${BENC_GEN} ${HIGH_CONFLICT} ${SMALL_WS} -passive-replication
${ISPN_GEN} ${ISPN_DEFAULT} ${ISPN_HC} -pb-protocol
run_test ${NR_NODES_TO_USE} "HI-SMALL-RC-PR-run8" ${EST_DURATION} ${CLUSTER}

${JGRP_GEN} -sequencer
${BENC_GEN} ${HIGH_CONFLICT} ${SMALL_WS}
${ISPN_GEN} ${ISPN_DEFAULT} ${ISPN_HC} -to-protocol
run_test ${NR_NODES_TO_USE} "HI-SMALL-RC-TO-run9" ${EST_DURATION} ${CLUSTER}

#Low conflict with small write set
${JGRP_GEN}
${BENC_GEN} ${LOW_CONFLICT} ${SMALL_WS}
${ISPN_GEN} ${ISPN_DEFAULT} ${ISPN_LC}
run_test ${NR_NODES_TO_USE} "LOW-SMALL-RC-2PC-run10" ${EST_DURATION} ${CLUSTER}

${BENC_GEN} ${LOW_CONFLICT} ${SMALL_WS} -passive-replication
${ISPN_GEN} ${ISPN_DEFAULT} ${ISPN_LC} -pb-protocol
run_test ${NR_NODES_TO_USE} "LOW-SMALL-RC-PR-run11" ${EST_DURATION} ${CLUSTER}

${JGRP_GEN} -sequencer
${BENC_GEN} ${LOW_CONFLICT} ${SMALL_WS}
${ISPN_GEN} ${ISPN_DEFAULT} ${ISPN_LC} -to-protocol
run_test ${NR_NODES_TO_USE} "LOW-SMALL-RC-TO-run12" ${EST_DURATION} ${CLUSTER}


echo "============ FINISHED BENCHMARKING ==============="

exit 0
