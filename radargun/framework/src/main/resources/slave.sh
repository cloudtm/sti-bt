#!/bin/bash

## Load includes
if [ "x$RADARGUN_HOME" = "x" ]; then DIRNAME=`dirname $0`; RADARGUN_HOME=`cd $DIRNAME/..; pwd` ; fi; export RADARGUN_HOME
. ${RADARGUN_HOME}/bin/includes.sh

set_env

MASTER_HOST=""
MASTER_PORT=""
LOG4J_PREFIX=`hostname`


default_master() {
  MASTER_HOST=`sed -n -e '/bindAddress/{
                           s/.*bindAddress="${//
                           s/}".*//
                           s/:.*//
                           p
                           }' ${RADARGUN_HOME}/conf/benchmark.xml`
  MASTER_PORT=`sed -n -e '/port="/{
                           s/.*port="${//
                           s/}".*//
                           s/:.*//
                           p
                           }' ${RADARGUN_HOME}/conf/benchmark.xml`
}

default_master
MASTER=${MASTER_HOST}:${MASTER_PORT}

help_and_exit() {
  echo "Usage: "
  echo '  $ slave.sh [-m host:port] [-p log4j_file_prefix] [-g gossip_router_hostname] [-l local_prefix]'
  echo ""
  echo "   -m        Master host and port. Optional, defaults to ${MASTER}. (this value is taken from ./conf/benchmark.xml)."
  echo ""
  echo "   -p        Prefix to be appended to the generated log4j file (useful when running multiple nodes on the same machine). Optional."
  echo ""
  echo "   -h        Displays this help screen"
  echo ""
  echo "   -g        Gossip Router hostname"
  echo ""
  echo "   -l        Sets a local prefix for std_out (useful when running multiple nodes on the same machine). Optional"
  echo ""
  wrappedecho "   -jmx     Set the JMX port for remote management. Default value is ${JMX_SLAVES_PORT}"
  wrappedecho ""
  exit 0
}

welcome "This script is used to launch the local slave process."

### read in any command-line params
while ! [ -z $1 ]
do
  case "$1" in
    "-m")
      MASTER=$2
      shift
      ;;
    "-p")
      LOG4J_PREFIX=$2
      shift
      ;;
    "-h")
      help_and_exit
      ;;
    "-g")
      GOSSIP_HOST=$2
      shift
      ;;
    "-l")
      LOCAL_PREFIX=$2
      shift
      ;;
    "-jmx")
      JMX_SLAVES_PORT=$2;
      shift
      ;;
    *)
      echo "Warn: unknown param \"${1}\"" 
      help_and_exit
      ;;
  esac
  shift
done

if [[ "${MASTER}" != *:* ]]; then
MASTER=${MASTER}":"${MASTER_PORT}
fi


CONF="-master $MASTER"

add_fwk_to_classpath

BIND_ADDRESS=`hostname`
D_VARS="-Djava.net.preferIPv4Stack=true" 
D_VARS="${D_VARS} -Dlog4j.file.prefix=${LOG4J_PREFIX}" 
D_VARS="${D_VARS} -Dbind.address=${BIND_ADDRESS}" 
D_VARS="${D_VARS} -Djgroups.bind_addr=${BIND_ADDRESS}"

if [ -n "${GOSSIP_HOST}" ]; then
D_VARS="${D_VARS} -Djgroups.gossip_host=${GOSSIP_HOST}"
fi

#enable	remote JMX
D_VARS="${D_VARS} -Dcom.sun.management.jmxremote.port=${JMX_SLAVES_PORT}"
D_VARS="${D_VARS} -Dcom.sun.management.jmxremote.authenticate=false"
D_VARS="${D_VARS} -Dcom.sun.management.jmxremote.ssl=false"


HOST_NAME=`hostname`
echo "java ${JVM_OPTS} ${D_VARS} -classpath $CP org.radargun.Slave ${CONF}" > ${LOCAL_PREFIX}stdout_slave_${HOST_NAME}.out
echo "--------------------------------------------------------------------------------" >> ${LOCAL_PREFIX}stdout_slave_${HOST_NAME}.out
nohup java ${JVM_OPTS} ${D_VARS} -classpath $CP org.radargun.Slave ${CONF} >> ${LOCAL_PREFIX}stdout_slave_${HOST_NAME}.out 2>&1 &
echo "... done! Slave process started on host ${HOST_NAME}!"
echo ""


