#!/bin/bash

#default values
if [ -n "${BENCH_XML_FILEPATH}" ]; then
  DEST_FILE=${BENCH_XML_FILEPATH}
else
  DEST_FILE=./benchmark.xml
fi

CLIENTS=1
LOCAL_THREADS=1
EXEC_MODE="NORMAL"
REMOTE_PROB=50
OP_PER_TX=5
KEYS_SIZE=10000
DURATION=20
CACHE_CONFIG_FILE="ispn.xml"
PARTIAL_REPLICATION="false"
PASSIVE_REPLICATION="false"

if [ -n "${ISPN_CONFIG_FILENAME}" ]; then
  CONFIGURATION_FILE=${ISPN_CONFIG_FILENAME}
else
  CONFIGURATION_FILE="config.xml" 
fi

help_and_exit(){
echo "usage: ${0} <options>"
echo "options:"
echo "  -c <value>               number of clients (also means number of nodes used)"
echo "                           default: ${CLIENTS}"
echo ""
echo "  -l <value>               number of threads per node"
echo "                           default: ${LOCAL_THREADS}"
echo ""
echo "  -e <value>               execution mode"
echo "                           default: ${EXEC_MODE}"
echo ""
echo "  -r <value>               probability of remote accesses"
echo "                           default: ${REMOTE_PROB}"
echo ""
echo "  -o <value>               operations per transaction"
echo "                           default: ${OP_PER_TX}"
echo ""
echo "  -k <value>        	 number of data items per node"
echo "                           default: ${KEYS_SIZE}"
echo ""
echo "  -d <value>        	 duration of the benchmark in seconds"
echo "                           default: ${DURATION}"
echo ""
echo "  -h                       show this message and exit"
exit 0
}

while [ -n $1 ]; do
case $1 in
  -h) help_and_exit;;
  -c) CLIENTS=$2; shift 2;;
  -l) LOCAL_THREADS=$2; shift 2;;
  -e) EXEC_MODE=$2; shift 2;;
  -r) REMOTE_PROB=$2; shift 2;;
  -o) OP_PER_TX=$2; shift 2;;
  -k) KEYS_SIZE=$2; shift 2;;
  -d) DURATION=$2; shift 2;;
  -passive-replication) PASSIVE_REPLICATION="true"; shift 1;;
  -distributed) PARTIAL_REPLICATION="true"; shift 1;;
  -*) echo "unknown option $1"; exit 1;;
  *) break;;
esac
done

echo "Writing configuration to ${DEST_FILE}"

echo "<bench-config>" > ${DEST_FILE}

echo "   <master" >> ${DEST_FILE}
echo "         bindAddress=\"\${127.0.0.1:master.address}\"" >> ${DEST_FILE}
echo "         port=\"\${21083:master.port}\"/>" >> ${DEST_FILE}

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

echo "      <BTreePopulation" >> ${DEST_FILE}
echo "            keysSize=\"${KEYS_SIZE}\" />" >> ${DEST_FILE}

echo "      <CacheSize" >> ${DEST_FILE}
echo "            statName=\"CACHE_SIZE_BEFORE_BENCH\" />" >> ${DEST_FILE}

echo "      <BTreeBenchmark" >> ${DEST_FILE}
echo "            localThreads=\"${LOCAL_THREADS}\"" >> ${DEST_FILE}
echo "            execMode=\"${EXEC_MODE}\"" >> ${DEST_FILE}
echo "            remoteProb=\"${REMOTE_PROB}\"" >> ${DEST_FILE}
echo "            opPerTx=\"${OP_PER_TX}\"" >> ${DEST_FILE}
echo "            keysSize=\"${KEYS_SIZE}\"" >> ${DEST_FILE}
echo "            seconds=\"${DURATION}\" />" >> ${DEST_FILE}

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
