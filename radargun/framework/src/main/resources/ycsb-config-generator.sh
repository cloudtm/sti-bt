#!/bin/bash

#default values
if [ -n "${BENCH_XML_FILEPATH}" ]; then
  DEST_FILE=${BENCH_XML_FILEPATH}
else
  DEST_FILE=./benchmark.xml
fi
CLIENTS=1
LOCAL_THREADS=1
READCOUNT=20
TIME=40
COUNT=1024
READONLY=50
REMOTE=5
TO="false"
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
echo "  -n <value>               number of operations per transaction"
echo "                           default: ${NUMBER}"
echo ""
echo "  -q <value>               the range of data to touch"
echo "                           default: ${QUERIES}"
echo ""
echo "  -r <value>               the size of the tables"
echo "                           default: ${RELATIONS}"
echo ""
echo "  -t <value>        	 the number of transactions"
echo "                           default: ${TRANSACTIONS}"
echo ""
echo "  -u <value>               the percentage of reservations"
echo "                           default: ${USER}"
echo ""
echo "  -ro <value>               the percentage of read-only"
echo "                           default: ${READONLY}"
echo ""
echo ""
echo "  -h                       show this message and exit"
exit 0
}

while [ -n $1 ]; do
case $1 in
  -h) help_and_exit;;
  -c) CLIENTS=$2; shift 2;;
  -l) LOCAL_THREADS=$2; shift 2;;
  -rc) READCOUNT=$2; shift 2;;
  -t) TIME=$2; shift 2;;
  -count) COUNT=$2; shift 2;;
  -ro) READONLY=$2; shift 2;;
  -rem) REMOTE=$2; shift 2;;
  -to) TO=$2; shift 2;;
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

echo "      <YCSBPopulation" >> ${DEST_FILE}
echo "            totalOrder=\"${TO}\"" >> ${DEST_FILE}
echo "            recordCount=\"${COUNT}\" />" >> ${DEST_FILE}

echo "      <CacheSize" >> ${DEST_FILE}
echo "            statName=\"CACHE_SIZE_BEFORE_BENCH\" />" >> ${DEST_FILE}

echo "      <YCSBBenchmark" >> ${DEST_FILE}
echo "            nodes=\"${CLIENTS}\"" >> ${DEST_FILE}
echo "            threads=\"${LOCAL_THREADS}\"" >> ${DEST_FILE}
echo "            multiplereadcount=\"${READCOUNT}\"" >> ${DEST_FILE}
echo "            executiontime=\"${TIME}\"" >> ${DEST_FILE}
echo "            recordcount=\"${COUNT}\"" >> ${DEST_FILE}
echo "            readonly=\"${READONLY}\"" >> ${DEST_FILE}
echo "            totalOrder=\"${TO}\"" >> ${DEST_FILE}
echo "            remote=\"${REMOTE}\"/>" >> ${DEST_FILE}

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
