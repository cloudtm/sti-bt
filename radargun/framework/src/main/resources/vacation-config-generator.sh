#!/bin/bash

#default values
if [ -n "${BENCH_XML_FILEPATH}" ]; then
  DEST_FILE=${BENCH_XML_FILEPATH}
else
  DEST_FILE=./benchmark.xml
fi

LOWER_BOUND=2
INTRA_NOD_CONC=true
THREAD_MIGRATION=true
GHOST_READS=true
COLOCATION=true
REPLICATION_DEGREES=true

NUMBER=4
QUERIES=60
RELATIONS=1024
TRANSACTIONS=2000
RO=50
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
exit 0
}

while [ -n $1 ]; do
case $1 in
  -h) help_and_exit;;
  -n) NUMBER=$2; shift 2;;
  -q) QUERIES=$2; shift 2;;
  -r) RELATIONS=$2; shift 2;;
  -time) TRANSACTIONS=$2; shift 2;;
  -t) THREAD_MIGRATION=$2; shift 2;;
  -ro) RO=$2; shift 2;;
  -g) GHOST_READS=$2; shift 2;;
  -l) COLOCATION=$2; shift 2;;
  -repl) REPLICATION_DEGREES=$2; shift 2;;
  -i) INTRA_NODE_CONC=$2; shift 2;;
  -b) LOWER_BOUND=$2; shift 2;;
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
echo "         port=\"\${17523:master.port}\"/>" >> ${DEST_FILE}

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

echo "      <VacationPopulation" >> ${DEST_FILE}
echo "            threadMigration=\"${THREAD_MIGRATION}\"" >> ${DEST_FILE}
echo "            ghostReads=\"${GHOST_READS}\"" >> ${DEST_FILE}
echo "            colocation=\"${COLOCATION}\"" >> ${DEST_FILE}
echo "            replicationDegrees=\"${REPLICATION_DEGREES}\"" >> ${DEST_FILE}
echo "            intraNodeConc=\"${INTRA_NODE_CONC}\"" >> ${DEST_FILE}
echo "		      lowerBound=\"${LOWER_BOUND}\"" >> ${DEST_FILE}
echo "            relations=\"${RELATIONS}\" />" >> ${DEST_FILE}

echo "      <CacheSize" >> ${DEST_FILE}
echo "            statName=\"CACHE_SIZE_BEFORE_BENCH\" />" >> ${DEST_FILE}

echo "      <VacationBenchmark" >> ${DEST_FILE}
echo "            number=\"${NUMBER}\"" >> ${DEST_FILE}
echo "            queries=\"${QUERIES}\"" >> ${DEST_FILE}
echo "            relations=\"${RELATIONS}\"" >> ${DEST_FILE}
echo "            transactions=\"${TRANSACTIONS}\"" >> ${DEST_FILE}
echo "            readOnly=\"${RO}\"/>" >> ${DEST_FILE}

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
