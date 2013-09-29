#!/bin/bash

if [ -n "${ISPN_CONFIG_FILEPATH}" ]; then
  DST_DIR=${ISPN_CONFIG_FILEPATH}
else
  DST_DIR=./infinispan.xml
fi
STATS="false"
JGR_CONFIG="jgroups/jgroups-udp.xml"
ISOLATION_LEVEL="READ_COMMITTED"
CONCURRENCY_LEVEL="32"
WRITE_SKEW="false"
LOCK_TIMEOUT="10000"
LOCKING_MODE="OPTIMISTIC"
TX_PROTOCOL="NORMAL"
TO_CORE_POOL_SIZE="8"
TO_MAX_POOL_SIZE="64"
TO_KEEP_ALIVE="10000"
TO_QUEUE_SIZE="100"
TO_1PC="false"
DEADLOCK_DETECTION="false"
CLUSTERING_MODE="r"
DIST_NUM_OWNERS="2"
VERSIONS="false"
VERSION_SCHEME="SIMPLE"

help() {
echo "usage: $0 <options>"
echo "  options:"
echo "    -dst-file <value>             the filepath where the configuration generated will be written"
echo "                                  default: ${DST_DIR}"
echo ""
echo "    -jgr-config <value>           the filepaht of jgroups configuration file"
echo "                                  default: ${JGR_CONFIG}"
echo ""
echo "    -isolation-level <value>      the transaction isolation level"
echo "                                  default: ${ISOLATION_LEVEL}"
echo ""
echo "    -concurrency-level <value>    the concurrency level for lock containers"
echo "                                  default: ${CONCURRENCY_LEVEL}"
echo ""
echo "    -lock-timeout <value>         maximum time to attempt a particular lock acquisition"
echo "                                  default: ${LOCK_TIMEOUT}"
echo ""
echo "    -to-core-pool-size <value>    total order thread pool configuration"
echo "                                  default: ${TO_CORE_POOL_SIZE}"
echo ""
echo "    -to-max-pool-size <value>     total order thread pool configuration"
echo "                                  default: ${TO_MAX_POOL_SIZE}"
echo ""
echo "    -to-keep-alive <value>        total order thread pool configuration"
echo "                                  default: ${TO_KEEP_ALIVE}"
echo ""
echo "    -to-queue-size <value>        total order thread pool configuration"
echo "                                  default: ${TO_QUEUE_SIZE}"
echo ""
echo "    -clustering-mode <value>      the clustering mode to use" 
echo "                                  values (r -- replicated, d -- distributed, i -- invalidation, l -- local)"
echo "                                  default: ${CLUSTERING_MODE}"
echo ""
echo "    -num-owner <value>            number of owners in distributed mode"
echo "                                  default: ${DIST_NUM_OWNERS}"
echo ""
echo "    -versioned                    enables the versioned cache"
echo ""
echo "    -write-skew                   enables the write skew check if the isolation level is REPEATABLE_READ"
echo ""
echo "    -to-protocol                  change the commit protoco to Total Order based"
echo ""
echo "    -pessimist-locking-mode       change the locking mode to pessimist"
echo ""
echo "    -deadlock-detector            enable the deadlock detection mechanism"
echo ""
echo "    -sync                         enable synchronous communication"
echo ""
echo "    -stats                        enable stats collection"
echo ""
echo "    -to-1pc                       enable one phase commit in Total Order protocol (if write skew is enabled)"
echo ""
echo "    -h                            show this message"
}

while [ -n $1 ]; do
case $1 in
  -h) help; exit 0;;
  -dst-file) DST_DIR=$2; shift 2;;
  -jgr-config) JGR_CONFIG=$2; shift 2;;
  -isolation-level) ISOLATION_LEVEL=$2; shift 2;;
  -concurrency-level) CONCURRENCY_LEVEL=$2; shift 2;;
  -lock-timeout) LOCK_TIMEOUT=$2; shift 2;;
  -to-core-pool-size) TO_CORE_POOL_SIZE=$2; shift 2;;
  -to-max-pool-size) TO_MAX_POOL_SIZE=$2; shift 2;;
  -to-keep-alive) TO_KEEP_ALIVE=$2; shift 2;;
  -clustering-mode) CLUSTERING_MODE=$2; shift 2;;
  -num-owner) DIST_NUM_OWNERS=$2; shift 2;;
  -versioned) VERSIONS="true"; shift 1;;
  -write-skew) WRITE_SKEW="true"; shift 1;;
  -to-protocol) TX_PROTOCOL="TOTAL_ORDER"; shift 1;;
  -pessimist-locking-mode) LOCKING_MODE="PESSIMISTIC"; shift 1;;
  -deadlock-detector) DEADLOCK_DETECTION="true"; shift 1;;
  -sync) SYNC=1; shift 1;;
  -stats) STATS="true"; shift 1;;
  -to-queue-size) TO_QUEUE_SIZE=$2; shift 2;;
  -to-1pc) TO_1PC="true"; shift 1;;
  -*) echo "unkown option $1"; help; exit 1;;
  *) break;;
  esac
done

echo "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" > ${DST_DIR}
echo "<infinispan" >> ${DST_DIR}
echo "        xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"" >> ${DST_DIR}
echo "        xsi:schemaLocation=\"urn:infinispan:config:5.1 http://www.infinispan.org/schemas/infinispan-config-5.1.xsd\"" >> ${DST_DIR}
echo "        xmlns=\"urn:infinispan:config:5.1\">" >> ${DST_DIR}

echo "    <global>" >> ${DST_DIR}

echo "        <globalJmxStatistics" >> ${DST_DIR}
echo "                enabled=\"${STATS}\"" >> ${DST_DIR}
echo "                jmxDomain=\"org.infinispan\"/>" >> ${DST_DIR}

echo "        <transport" >> ${DST_DIR}
echo "                clusterName=\"infinispan-cluster\">" >> ${DST_DIR}
echo "            <properties>" >> ${DST_DIR}
echo "                <property" >> ${DST_DIR}
echo "                        name=\"configurationFile\"" >> ${DST_DIR}
echo "                        value=\"${JGR_CONFIG}\" />" >> ${DST_DIR}
echo "            </properties>" >> ${DST_DIR}
echo "        </transport>" >> ${DST_DIR}
echo "    </global>" >> ${DST_DIR}

echo "    <default>" >> ${DST_DIR}

echo "        <locking" >> ${DST_DIR}
echo "                isolationLevel=\"${ISOLATION_LEVEL}\"" >> ${DST_DIR}
echo "                concurrencyLevel=\"${CONCURRENCY_LEVEL}\"" >> ${DST_DIR}
echo "                writeSkewCheck=\"${WRITE_SKEW}\"" >> ${DST_DIR}
echo "                useLockStriping=\"false\"" >> ${DST_DIR}
echo "                lockAcquisitionTimeout=\"${LOCK_TIMEOUT}\"/>" >> ${DST_DIR}

echo "        <transaction" >> ${DST_DIR}
echo "                transactionManagerLookupClass=\"org.infinispan.transaction.lookup.GenericTransactionManagerLookup\"" >> ${DST_DIR}
echo "                useEagerLocking=\"false\"" >> ${DST_DIR}
echo "                transactionMode=\"TRANSACTIONAL\"" >> ${DST_DIR}
echo "                syncRollbackPhase=\"false\"" >> ${DST_DIR}
echo "                cacheStopTimeout=\"30000\"" >> ${DST_DIR}
echo "                useSynchronization=\"false\"" >> ${DST_DIR}
echo "                syncCommitPhase=\"false\"" >> ${DST_DIR}
echo "                lockingMode=\"${LOCKING_MODE}\"" >> ${DST_DIR}
echo "                eagerLockSingleNode=\"false\"" >> ${DST_DIR}
echo "                use1PcForAutoCommitTransactions=\"false\"" >> ${DST_DIR}
echo "                autoCommit=\"true\"" >> ${DST_DIR}
echo "                transactionProtocol=\"${TX_PROTOCOL}\">" >> ${DST_DIR}
echo "            <totalOrderThreading" >> ${DST_DIR}
echo "                    corePoolSize=\"${TO_CORE_POOL_SIZE}\"" >> ${DST_DIR}
echo "                    maximumPoolSize=\"${TO_MAX_POOL_SIZE}\"" >> ${DST_DIR}
echo "                    keepAliveTime=\"${TO_KEEP_ALIVE}\"" >> ${DST_DIR}
echo "                    queueSize=\"${TO_QUEUE_SIZE}\"" >> ${DST_DIR}
echo "                    onePhaseCommit=\"${TO_1PC}\"/>" >> ${DST_DIR}
echo "        </transaction>" >> ${DST_DIR}

echo "        <jmxStatistics" >> ${DST_DIR}
echo "                enabled=\"${STATS}\"/>" >> ${DST_DIR}

echo "        <deadlockDetection" >> ${DST_DIR}
echo "                enabled=\"${DEADLOCK_DETECTION}\"/>" >> ${DST_DIR}

echo "        <clustering mode=\"${CLUSTERING_MODE}\">" >> ${DST_DIR}

if [ -n "${SYNC}" ]; then
echo "            <sync" >> ${DST_DIR}
echo "                    replTimeout=\"15000\" />" >> ${DST_DIR}
else
echo "            <async" >> ${DST_DIR}
echo "                    replQueueMaxElements=\"1000\"" >> ${DST_DIR}
echo "                    replQueueClass=\"org.infinispan.remoting.ReplicationQueueImpl\"" >> ${DST_DIR}
echo "                    useReplQueue=\"false\"" >> ${DST_DIR}
echo "                    replQueueInterval=\"5000\"" >> ${DST_DIR}
echo "                    asyncMarshalling=\"false\" />" >> ${DST_DIR}
fi

if [ "${CLUSTERING_MODE}" == "r" -o "${CLUSTERING_MODE}" == "i" ]; then
echo "            <stateRetrieval" >> ${DST_DIR}
echo "                    alwaysProvideInMemoryState=\"false\"" >> ${DST_DIR}
echo "                    fetchInMemoryState=\"false\"" >> ${DST_DIR}
echo "                    numRetries=\"5\"" >> ${DST_DIR}
echo "                    retryWaitTimeIncreaseFactor=\"2\"" >> ${DST_DIR}
echo "                    initialRetryWaitTime=\"500\"" >> ${DST_DIR}
echo "                    logFlushTimeout=\"60000\"" >> ${DST_DIR}
echo "                    timeout=\"240000\"/>" >> ${DST_DIR}
fi
if [ "${CLUSTERING_MODE}" == "d" ]; then
echo "           <hash" >> ${DST_DIR}
#echo "                    class=\"null\"" >> ${DST_DIR}
echo "                    numVirtualNodes=\"1\"" >> ${DST_DIR}
echo "                    numOwners=\"${DIST_NUM_OWNERS}\"" >> ${DST_DIR}
echo "                    rehashEnabled=\"false\"" >> ${DST_DIR}
echo "                    rehashRpcTimeout=\"600000\"" >> ${DST_DIR}
#echo "                    hashFunctionClass=\"org.infinispan.commons.hash.MurmurHash3\"" >> ${DST_DIR}
echo "                    rehashWait=\"60000\" />" >> ${DST_DIR}

echo "           <l1" >> ${DST_DIR}
echo "                    enabled=\"false\"" >> ${DST_DIR}
echo "                    onRehash=\"false\"" >> ${DST_DIR}
echo "                    lifespan=\"600000\"" >> ${DST_DIR}
echo "                    invalidationThreshold=\"0\" />" >> ${DST_DIR}
fi
echo "        </clustering>" >> ${DST_DIR}

echo "        <versioning" >> ${DST_DIR}
echo "                enabled=\"${VERSIONS}\"" >> ${DST_DIR}
echo "                versioningScheme=\"${VERSION_SCHEME}\" />" >> ${DST_DIR}
echo "    </default>" >> ${DST_DIR}
echo "</infinispan>" >> ${DST_DIR}



