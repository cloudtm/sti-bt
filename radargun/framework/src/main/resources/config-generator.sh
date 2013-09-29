#!/bin/bash

WORKING_DIR=`cd $(dirname $0); cd ..; pwd`

DEST_FILE=${WORKING_DIR}/conf/benchmark.xml

PARTIAL_REPLICATION="false"
NUMBER_OF_THREADS="2"
#in seconds
SIMULATION_TIME="300"
CACHE_CONFIG_FILE="ispn.xml"
NUMBER_OF_WAREHOUSES="1"
C_LAST_MASK="0"
OL_ID_MASK="0"
C_ID_MASK="0"
ARRIVAL_RATE="0"
PAYMENT_WEIGHT="45"
ORDER_STATUS_WEIGHT="5"
PARALLEL_POPULATION="true"
POPULATION_THREADS="2"
POPULATION_BATCH_LEVEL="100"
SAME_WAREHOUSE="false"
PASSIVE_REPLICATION="false"
PRELOAD="false"
STAT_SAMPLE_INTERVAL="0"

help_and_exit(){
echo "usage: ${0} <options>"
echo "options:"
echo "  -dest-file <value>               the file path where the generated configuration will be written"
echo "                                   default: ${DEST_FILE}"
echo ""
echo "  -simul-time <value>              simulation time (in seconds)"
echo "                                   default: ${SIMULATION_TIME}"
echo ""
echo "  -nr-thread <value>               the number of threads executing transactions in each node"
echo "                                   default: ${NUMBER_OF_THREADS}"
echo ""
echo "  -nr-warehouse <value>            the number of warehouses"
echo "                                   default: ${NUMBER_OF_WAREHOUSES}"
echo ""
echo "  -c-last-mask <value>             the mask used to generate non-uniformly distributed random customer last names"
echo "                                   default: ${C_LAST_MASK}"
echo ""
echo "  -ol-id-mask <values>             mask used to generate non-uniformly distributed random item numbers"
echo "                                   default: ${OL_ID_MASK}"
echo ""
echo "  -c-id-mask <value>               mask used to generate non-uniformly distributed random customer numbers"
echo "                                   default: ${C_ID_MASK}"
echo ""
echo "  -arrival-rate <value>            if the value is greater than 0.0, the "open system" mode is active and the" 
echo "                                   parameter represents the arrival rate (in transactions per second) of a job"
echo "                                   to the system; otherwise the "closed system" mode is active and each thread" 
echo "                                   generates and executes a new transaction in an iteration as soon as it has" 
echo "                                   completed the previous iteration"
echo "                                   default: ${ARRIVAL_RATE}"
echo ""
echo "  -payment-weight <value>          percentage of Payment transactions"
echo "                                   default: ${PAYMENT_WEIGHT}"
echo ""
echo "  -order-status-weight <value>     percentage of Order Status transactions" 
echo "                                   default: ${ORDER_STATUS_WEIGHT}"
echo ""
echo "  -population-threads <value>      the number of populating threads per node"
echo "                                   default: ${POPULATION_THREADS}"
echo ""
echo "  -population-batch-level <value>  the size of a transaction in population (the number of items per transaction)"  
echo "                                   default: ${POPULATION_BATCH_LEVEL}"
echo ""
echo "  -nr-items-inter <value>          the interval of the possible number of items, in the format min,max. This will"
echo "                                   be used to chooses the number of items in the New Order transaction types"
echo "                                   default: 5,15"
echo ""
echo "  -config <value>                  the path for the configuration of the cache"
echo "                                   default: ${CACHE_CONFIG_FILE}"
echo ""
echo "  -stat-sample-interval <value>    the period (in milliseconds) in which the CPU and memory usage is collected"
echo "                                   A value less or equals than 0 disables the collection"
echo "                                   default: ${STAT_SAMPLE_INTERVAL}"
echo ""
echo "  -preload-from-db                 it means that the cache is already populated. This will skip the population phase"
echo "                                   default: population is performed."
echo "                                   NOTE: don't remote the population phase from configuration. Only set it to already populated"
echo ""
echo "  -no-parallel-population          disables the parallel population (the fastest)"
echo "                                   default: is enabled"
echo ""
echo "  -distributed                     set the configuration to use distributed mode"
echo "                                   default: is set to replicated mode"
echo ""
echo "  -passive-replication             set the configuration to use passive replication"
echo "                                   default: use a default scheme"
echo ""
echo "  -same-warehouse-access           each node picks a warehouse in the beginning and works over that warehouse"
echo "                                   default: picks a random warehouse when the transaction starts"
echo ""
echo "  -h                               show this message and exit"
exit 0
}

while [ -n "$1" ]; do
case $1 in
  -h) help_and_exit;;
  -dest-file) DEST_FILE=$2; shift 2;;
  -simul-time) SIMULATION_TIME=$2; shift 2;;  
  -nr-thread) NUMBER_OF_THREADS=$2; shift 2;;  
  -config) CACHE_CONFIG_FILE=$2; shift 2;;
  -nr-warehouse) NUMBER_OF_WAREHOUSES=$2; shift 2;;
  -c-last-mask) C_LAST_MASK=$2; shift 2;;
  -ol-id-mask) OL_ID_MASK=$2; shift 2;;
  -c-id-mask) C_ID_MASK=$2; shift 2;;
  -arrival-rate) ARRIVAL_RATE=$2; shift 2;;
  -payment-weight) PAYMENT_WEIGHT=$2; shift 2;;
  -order-status-weight) ORDER_STATUS_WEIGHT=$2; shift 2;;
  -population-threads) POPULATION_THREADS=$2; shift 2;;
  -population-batch-level) POPULATION_BATCH_LEVEL=$2; shift 2;;
  -no-parallel-population) PARALLEL_POPULATION="false"; shift 1;;
  -distributed) PARTIAL_REPLICATION="true"; shift 1;;
  -passive-replication) PASSIVE_REPLICATION="true"; shift 1;;
  -same-warehouse-access) SAME_WAREHOUSE="true"; shift 1;;
  -preload-from-db) PRELOAD="true"; shift 1;;
  -nr-items-inter) NR_ITEMS_INTERVAL=$2; shift 2;;
  -stat-sample-interval) STAT_SAMPLE_INTERVAL=$2; shift 2;;
  -*) echo "WARNING: unknown option '$1'. It will be ignored" >&2; shift 1;;
  *) echo "WARNING: unknown parameter '$1'. It will be ignored"; shift 1;;
esac
done

echo "Writing configuration to ${DEST_FILE}"

echo "<bench-config>" > ${DEST_FILE}

echo "   <master" >> ${DEST_FILE}
echo "         bindAddress=\"\${127.0.0.1:master.address}\"" >> ${DEST_FILE}
echo "         port=\"\${21032:master.port}\"/>" >> ${DEST_FILE}

echo "   <benchmark" >> ${DEST_FILE}
echo "         initSize=\"\${10:Islaves}\"" >> ${DEST_FILE}
echo "         maxSize=\"\${10:slaves}\"" >> ${DEST_FILE}
echo "         increment=\"1\">" >> ${DEST_FILE}

echo "      <DestroyWrapper" >> ${DEST_FILE}
echo "            runOnAllSlaves=\"true\"/>" >> ${DEST_FILE}

echo "      <StartCluster" >> ${DEST_FILE}
echo "            staggerSlaveStartup=\"true\"" >> ${DEST_FILE}
echo "            delayAfterFirstSlaveStarts=\"5000\"" >> ${DEST_FILE}
echo "            delayBetweenStartingSlaves=\"1000\"/>" >> ${DEST_FILE}

echo "      <ClusterValidation" >> ${DEST_FILE}
echo "            passiveReplication=\"${PASSIVE_REPLICATION}\"" >> ${DEST_FILE}
echo "            partialReplication=\"${PARTIAL_REPLICATION}\"/>" >> ${DEST_FILE}

echo "      <TpccPopulation" >> ${DEST_FILE}
echo "            preloadedFromDB=\"${PRELOAD}\"" >> ${DEST_FILE}
echo "            numWarehouses=\"${NUMBER_OF_WAREHOUSES}\"" >> ${DEST_FILE}
echo "            cLastMask=\"${C_LAST_MASK}\"" >> ${DEST_FILE}
echo "            olIdMask=\"${OL_ID_MASK}\"" >> ${DEST_FILE}
echo "            cIdMask=\"${C_ID_MASK}\"" >> ${DEST_FILE}
echo "            threadParallelLoad=\"${PARALLEL_POPULATION}\"" >> ${DEST_FILE} 
echo "            batchLevel=\"${POPULATION_BATCH_LEVEL}\"" >> ${DEST_FILE}
echo "            numLoaderThreads=\"${POPULATION_THREADS}\"/>" >> ${DEST_FILE}

echo "      <CacheSize" >> ${DEST_FILE}
echo "            statName=\"CACHE_SIZE_BEFORE_BENCH\" />" >> ${DEST_FILE}

echo "      <TpccBenchmark" >> ${DEST_FILE}

if [ -n "${NR_ITEMS_INTERVAL}" ]; then
echo "            numberOfItemsInterval=\"${NR_ITEMS_INTERVAL}\"" >> ${DEST_FILE}
fi

echo "            statsSamplingInterval=\"${STAT_SAMPLE_INTERVAL}\"" >> ${DEST_FILE}
echo "            numOfThreads=\"${NUMBER_OF_THREADS}\"" >> ${DEST_FILE}
echo "            perThreadSimulTime=\"${SIMULATION_TIME}\"" >> ${DEST_FILE}
echo "            arrivalRate=\"${ARRIVAL_RATE}\"" >> ${DEST_FILE}
echo "            accessSameWarehouse=\"${SAME_WAREHOUSE}\"" >> ${DEST_FILE}
echo "            paymentWeight=\"${PAYMENT_WEIGHT}\"" >> ${DEST_FILE}
echo "            orderStatusWeight=\"${ORDER_STATUS_WEIGHT}\"/>" >> ${DEST_FILE}

echo "      <CacheSize" >> ${DEST_FILE}
echo "            statName=\"CACHE_SIZE_AFTER_BENCH\" />" >> ${DEST_FILE}

echo "      <CsvReportGeneration/>" >> ${DEST_FILE}

echo "   </benchmark>" >> ${DEST_FILE}

echo "   <products>" >> ${DEST_FILE}

echo "      <infinispan4>" >> ${DEST_FILE}

echo "         <config name=\"${CACHE_CONFIG_FILE}\"/>" >> ${DEST_FILE}

echo "      </infinispan4>" >> ${DEST_FILE}

echo "   </products>" >> ${DEST_FILE}

echo "   <reports>" >> ${DEST_FILE}

echo "      <report name=\"Reports\" />" >> ${DEST_FILE}

echo "   </reports>" >> ${DEST_FILE}

echo "</bench-config>" >> ${DEST_FILE}

echo "Finished!"