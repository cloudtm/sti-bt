# ENVIRONMENT VARIABLES
MASTER=cloudtm.ist.utl.pt # this was modified!!!

#variables to set:
#CLUSTER=`echo vm{47..86}`
CLUSTER=`cat /home/ndiegues/machines` # `echo node{01..10}`
MONITOR_OUT=/home/ndiegues/radargun/www/files/report.csv
RADARGUN_DIR=/home/ndiegues/radargun/target/distribution/RadarGun-1.1.0-SNAPSHOT
RESULTS_DIR=/home/ndiegues/radargun/results-radargun
MONITOR_PATH=/home/ndiegues/radargun/csv-reporter

#uncoment to start gossip router (used in futuregrid)
GOSSIP_ROUTER=1
#uncomment to collect data placement stats
#DATA_PLACEMENT=1
#uncomment to collect the final keys value (if available)
#KEYS=1
#uncomment to collect the logs from all nodes
#LOGS=1
#uncomment to collect the the report from csv monitor
#MONITOR=1

LOGS_DIR=${RADARGUN_DIR}/logs
KEYS_DIR=${RADARGUN_DIR}/keys
MONITOR_DIR=${RADARGUN_DIR}/monitor
DATA_PLACEMENT_DIR=${RADARGUN_DIR}/dataplacement

ISPN_GEN=${RADARGUN_DIR}/plugins/infinispan4/bin/config-generator.sh
JGRP_GEN=${RADARGUN_DIR}/plugins/infinispan4/bin/jgrp-generator.sh
BENC_GEN=${RADARGUN_DIR}/bin/btree-config-generator.sh

export BENCH_XML_FILEPATH=${RADARGUN_DIR}/conf/benchmark.xml
export ISPN_CONFIG_FILENAME=ispn.xml
export ISPN_CONFIG_FILEPATH=${RADARGUN_DIR}/plugins/infinispan4/conf/${ISPN_CONFIG_FILENAME}

RC="READ_COMMITTED"
RR="REPEATABLE_READ"

copy_to_all() {
#for node in $@; do
#echo "copy to ${node}"
#if [ "${MASTER}" == "${node}" ]; then
#echo "not copying... is the master!"
#else
parallel-scp -r -h /home/ndiegues/machines /home/ndiegues/radargun/target/distribution/RadarGun-1.1.0-SNAPSHOT/ /home/ndiegues/radargun/target/distribution/
#scp -r ${RADARGUN_DIR}/* ${node}:${RADARGUN_DIR} > /dev/null
#fi
#done
}

kill_java() {
for node in $@; do
echo "killing java process from ${node}"
ssh ${node} "killall -9 java"
done
}

save_logs() {
if [ "${LOGS}" == "1" ]; then
nr_nodes=${1}
shift
mkdir -p ${LOGS_DIR}/${nr_nodes}nodes
for node in $@; do
echo "save logs from ${node}"
scp ${node}:${RADARGUN_DIR}/*.out ${LOGS_DIR}/${nr_nodes}nodes/ > /dev/null
done
fi
}

save_dataplacement() {
if [ "${DATA_PLACEMENT}" == "1" ]; then
nr_nodes=${1}
shift
mkdir -p ${DATA_PLACEMENT_DIR}/${nr_nodes}nodes
for node in $@; do
echo "save data placement stats from ${node}"
scp ${node}:${RADARGUN_DIR}/stats.csv ${DATA_PLACEMENT_DIR}/${nr_nodes}nodes/${node}.csv > /dev/null
done
fi
}

save_keys() {
if [ "${KEYS}" == "1" ]; then
nr_nodes=${1}
shift
mkdir -p ${KEYS_DIR}/${nr_nodes}nodes
for node in $@; do
echo "save key from ${node}"
scp "${node}:${RADARGUN_DIR}/keys-*" ${KEYS_DIR}/${nr_nodes}-nodes/ > /dev/null
done
for node in $@; do
ssh ${node} "rm ${RADARGUN_DIR}/keys-*"
done
fi
}

save_monitor() {
if [ "${MONITOR}" == "1" ]; then
nr_nodes=${1}
shift
mkdir -p ${MONITOR_DIR}/${nr_nodes}nodes
echo "copy monitor stats from ${MONITOR_OUT} to ${MONITOR_DIR}/${nr_nodes}nodes"
cp ${MONITOR_OUT} ${MONITOR_DIR}/${nr_nodes}nodes/monitor.csv
fi
}

clean_slaves() {
for node in $@; do
echo "cleaning slave ${node}"
if [ "${MASTER}" == "${node}" ]; then
echo "not cleaning... is the master!"
else
ssh ${node} "rm -r ${RADARGUN_DIR}/*" > /dev/null
fi
done
}

clean_master() {
rm -r ${RADARGUN_DIR}/reports/*
rm ${RADARGUN_DIR}/*
rm -r ${LOGS_DIR}/*
rm -r ${KEYS_DIR}/*
rm -r ${DATA_PLACEMENT_DIR}/*
rm -r ${MONITOR_DIR}/*
killall -9 java
}

start_gossip_router() {
echo "start gossip router in ${MASTER}"
java -cp ${RADARGUN_DIR}/plugins/infinispan4/lib/jgroups*.jar org.jgroups.stack.GossipRouter -port 13248 -bindaddress cloudtm > /dev/null &
}

wait_until_test_finish() {
local MASTER_PID="";
#30 minutes max waiting time (+ estimated test duration)
for ((j = 0; j < 60; ++j)); do
MASTER_PID=`ps -ef | grep "org.radargun.LaunchMaster" | grep -v "grep" | grep "ndiegues" | awk '{print $2}'`
echo "Checking if the master finished..."
if [ -z "${MASTER_PID}" ]; then
echo "Master finished! No PID found! returning... @" `date`
return;
fi
echo "Master is running. PID is ${MASTER_PID}. @" `date`
sleep 30s
done
echo "Timeout!! Master is running. PID is ${MASTER_PID}. @" `date`
}


run_test() {
echo "======================================================================="
echo "Radargun test"

nr_nodes=${1};
shift
file_prefix=${1};
shift
estimated_duration=${1}
shift
nodes=$@;

echo "run with nodes: ${nr_nodes}"
echo "folder suffix: ${file_prefix}"
echo "estimated duration: ${estimated_duration} minutes"
echo "run in nodes: ${nodes}"

echo "======================================================================="
#return
echo "copy to all nodes"
copy_to_all ${nodes}

echo "starting tests with this number of nodes: ${nr_nodes}"
for i in $(echo "${nr_nodes}" | tr "," "\n"); do

if [ -n "${GOSSIP_ROUTER}" ]; then
	start_gossip_router
fi

if [ -n "${MONITOR}" ]; then
ln -sf ${MONITOR_PATH}/config-${i}.properties ${MONITOR_PATH}/config.properties
fi

echo "start test with ${i} number of nodes"
cd ${RADARGUN_DIR}
./bin/benchmark.sh -m ${MASTER} -i ${i} ${nodes}
echo "started at" $(date);
echo "wait ${estimated_duration} minutes";
sleep ${estimated_duration}m;

wait_until_test_finish

echo "kill all java";
./bin/master.sh -stop
#kill_java ${nodes}
save_logs ${i} ${nodes}
save_keys ${i} ${nodes}
save_dataplacement ${i} ${nodes}
save_monitor ${i}
done;

echo "all tests finishes... getting data..."
results="${RESULTS_DIR}/test-result-${file_prefix}"
mkdir -p ${results}
cd ${results}
echo "Copying reports to ${results}"
cp ${RADARGUN_DIR}/reports/*.csv . > /dev/null
echo "Copying Logs"
mkdir logs
cp -r ${LOGS_DIR}/* logs/ > /dev/null
echo "Copying Keys"
mkdir keys
cp -r ${KEYS_DIR}/* keys/ > /dev/null
echo "Copying Data Placement stats"
mkdir dataplacement
cp -r ${DATA_PLACEMENT_DIR}/* dataplacement/ > /dev/null
echo "Copying Monitor stats"
mkdir monitor
cp -r ${MONITOR_DIR}/* monitor/ > /dev/null
echo "Copying configuration files"
mkdir config
cp ${RADARGUN_DIR}/conf/benchmark.xml config/ > /dev/null
cp ${RADARGUN_DIR}/plugins/infinispan4/conf/ispn.xml config/ > /dev/null
cp ${RADARGUN_DIR}/plugins/infinispan4/conf/jgroups/jgroups.xml config/ > /dev/null

echo "all complete! cleaning nodes..."
#clean_master
#clean_slaves ${nodes}
}

