#!/bin/bash

#default values
if [ -n "${BENCH_XML_FILEPATH}" ]; then
  DEST_FILE=${BENCH_XML_FILEPATH}
else
  DEST_FILE=./benchmark.xml
fi
DISTRIBUTION="true"
SIMULATION_TIME=300
NUMBER_OF_KEYS="1000"
VALUE_SIZE="1000"
NUMBER_OF_THREADS="8"
WRITE_PERCENTAGE="10"
MIN_NUM_OF_OPS_PER_TX="10"
MAX_NUM_OF_OPS_PER_TX="10"
COORDINATOR_PARTICIPATION="true"
READ_ONLY_ALLOWED="true"
NO_CONTENTION="false"

if [ -n "${ISPN_CONFIG_FILENAME}" ]; then
  CONFIGURATION_FILE=${ISPN_CONFIG_FILENAME}
else
  CONFIGURATION_FILE="config.xml" 
fi

help_and_exit(){
echo "usage: ${0} <options>"
echo "options:"
echo "  -dest-file <value>               the file path where the generated configuration will be written"
echo "                                   default: ${DEST_FILE}"
echo ""
echo "  -simul-time <value>              simulation time (in seconds)"
echo "                                   default: ${SIMULATION_TIME}"
echo ""
echo "  -nr-keys <value>                 number of keys"
echo "                                   default: ${NUMBER_OF_KEYS}"
echo ""
echo "  -value-size <value>              the size of the value of each key (in bytes)"
echo "                                   default: ${VALUE_SIZE}"
echo ""
echo "  -nr-thread <value>               the number of threads executing transactions in each node"
echo "                                   default: ${NUMBER_OF_THREADS}"
echo ""
echo "  -write-percentage <value>        percentage of write transactions (0 to 100)"
echo "                                   default: ${WRITE_PERCENTAGE}"
echo ""
echo "  -config <value>                  the path for the configuration of the cache"
echo "                                   default: ${CONFIGURATION_FILE}"
echo ""
echo "  -min-op <value>                  minimum number of operations to be executed per transaction"
echo "                                   default: ${MIN_NUM_OF_OPS_PER_TX}"
echo ""
echo "  -max-op <value>                  maximum number of operations to be executed per transaction"
echo "                                   default: ${MAX_NUM_OF_OPS_PER_TX}"
echo ""
echo "  -no-coordinator-participation    the coordinator doesn't executes transactions"
echo "                                   default: coordinator execute transactions"
echo ""
echo "  -read-only                       allow the execution of read only transactions"
echo "                                   default: is is not allowed read only transactions, even if the write percentage is zero"
echo ""
echo "  -no-contention                   each thread has it owns keys and it has no conflicts between then"
echo "                                   default: contention can happen"
echo ""
echo "  -d                               set the configuration to use distributed mode"
echo "                                   default: is set to replicated mode"
echo ""
echo "  -get-keys                        save the keys (and their values) in the end of the benchmark"
echo ""
echo "  -h                               show this message and exit"
exit 0
}

while [ -n $1 ]; do
case $1 in
  -h) help_and_exit;;
  -dest-file) DEST_FILE=$2; shift 2;;
  -simul-time) SIMULATION_TIME=$2; shift 2;;
  -nr-keys) NUMBER_OF_KEYS=$2; shift 2;;
  -value-size) VALUE_SIZE=$2; shift 2;;
  -nr-thread) NUMBER_OF_THREADS=$2; shift 2;;
  -write-percentage) WRITE_PERCENTAGE=$2; shift 2;;
  -config) CONFIGURATION_FILE=$2; shift 2;;
  -min-op) MIN_NUM_OF_OPS_PER_TX=$2; shift 2;;
  -max-op) MAX_NUM_OF_OPS_PER_TX=$2; shift 2;;
  -no-coordinator-participation) COORDINATOR_PARTICIPATION="false"; shift 1;;
  -read-only) READ_ONLY_ALLOWED="true"; shift 1;;
  -no-contention) NO_CONTENTION="true"; shift 1;;
  -d) DISTRIBUTION="true"; shift 1;;
  -get-keys) GET_KEYS=1; shift 1;;
  -*) echo "unknown option $1"; exit 1;;
  *) break;;
esac
done

let SIMULATION_TIME=${SIMULATION_TIME}*1000000000


echo "<bench-config>" > ${DEST_FILE}

echo "    <master bindAddress=\"\${127.0.0.1:master.address}\" port=\"\${2103:master.port}\"/>" >> ${DEST_FILE}

echo "    <benchmark" >> ${DEST_FILE}
echo "            initSize=\"\${10:Islaves}\"" >> ${DEST_FILE}
echo "            maxSize=\"\${10:slaves}\"" >> ${DEST_FILE}
echo "            increment=\"1\">" >> ${DEST_FILE}

echo "        <DestroyWrapper" >> ${DEST_FILE}
echo "                runOnAllSlaves=\"true\"/>" >> ${DEST_FILE}

echo "        <StartCluster" >> ${DEST_FILE}
echo "                staggerSlaveStartup=\"true\"" >> ${DEST_FILE}
echo "                delayAfterFirstSlaveStarts=\"5000\"" >> ${DEST_FILE}
echo "                delayBetweenStartingSlaves =\"500\"/>" >> ${DEST_FILE}

echo "        <ClusterValidation" >> ${DEST_FILE}
echo "                partialReplication=\"${DISTRIBUTION}\"/>" >> ${DEST_FILE}

echo "        <Init" >> ${DEST_FILE}
echo "                noContentionEnabled=\"${NO_CONTENTION}\"" >> ${DEST_FILE}
echo "                numberOfKeys=\"${NUMBER_OF_KEYS}\"" >> ${DEST_FILE}
echo "                sizeOfValue=\"${VALUE_SIZE}\"" >> ${DEST_FILE}
echo "                numOfThreads=\"${NUMBER_OF_THREADS}\"/>" >> ${DEST_FILE}

echo "        <WebSessionBenchmark" >> ${DEST_FILE}
echo "                perThreadSimulTime=\"${SIMULATION_TIME}\"" >> ${DEST_FILE}
echo "                opsCountStatusLog=\"5000\"" >> ${DEST_FILE}
echo "                numberOfKeys =\"${NUMBER_OF_KEYS}\"" >> ${DEST_FILE}
echo "                sizeOfAnAttribute=\"${VALUE_SIZE}\"" >> ${DEST_FILE}
echo "                numOfThreads=\"${NUMBER_OF_THREADS}\"" >> ${DEST_FILE}
echo "                writePercentage=\"${WRITE_PERCENTAGE}\"" >> ${DEST_FILE}
echo "                lowerBoundOp=\"${MIN_NUM_OF_OPS_PER_TX}\"" >> ${DEST_FILE}
echo "                upperBoundOp=\"${MAX_NUM_OF_OPS_PER_TX}\"" >> ${DEST_FILE}
echo "                coordinatorParticipation=\"${COORDINATOR_PARTICIPATION}\"" >> ${DEST_FILE}
echo "                readOnlyTransactionsEnabled=\"${READ_ONLY_ALLOWED}\"" >> ${DEST_FILE}
echo "                noContentionEnabled=\"${NO_CONTENTION}\"/>" >> ${DEST_FILE}

if [ -n "${GET_KEYS}" ]; then
echo "        <GetKeys/>" >> ${DEST_FILE}
fi
echo "        <CsvReportGeneration/>" >> ${DEST_FILE}
echo "    </benchmark>" >> ${DEST_FILE}

echo "    <products>" >> ${DEST_FILE}
echo "        <infinispan4>" >> ${DEST_FILE}
echo "            <config name=\"${CONFIGURATION_FILE}\"/>" >> ${DEST_FILE}
echo "        </infinispan4>" >> ${DEST_FILE}
echo "    </products>" >> ${DEST_FILE}

echo "    <reports>" >> ${DEST_FILE}
#echo "        <report name=\"Benchmark Report\">" >> ${DEST_FILE}
#echo "            <item product=\"infinispan4\" config=\"${CONFIGURATION_FILE}\"/>" >> ${DEST_FILE}
#echo "        </report>" >> ${DEST_FILE}
echo "    </reports>">> ${DEST_FILE}

echo "</bench-config>" >> ${DEST_FILE}
