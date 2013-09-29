#!/bin/sh
###### This script is designed to be called from other scripts, to set environment variables including the bind
###### for cache products, as well as any JVM options.

### Set your bind address for the tests to use. Could be an IP, host name or a reference to an environment variable.
BIND_ADDRESS=${MYTESTIP_2}
JG_FLAGS="-Dresolve.dns=false -Djgroups.timer.num_threads=4"
JVM_OPTS="-server"
#JVM_OPTS="$JVM_OPTS -Xmx1G -Xms1G"
#allocate more memory if needed
JVM_OPTS="$JVM_OPTS -Xmx4G -Xms2G"
#JVM_OPTS="$JVM_OPTS -Xmx16G -Xms16G"
#choose on of the GC types (or none if you want to use the default)
JVM_OPTS="$JVM_OPTS -XX:+UseConcMarkSweepGC -XX:+CMSIncrementalMode"
#JVM_OPTS="$JVM_OPTS -XX:+UseParallelGC -XX:+UseParallelOldGC"
JVM_OPTS="$JVM_OPTS $JG_FLAGS"
JPROFILER_HOME=${HOME}/jprofiler6
JPROFILER_CFG_ID=103
JMX_MASTER_PORT="23042"
JMX_SLAVES_PORT="28395"

