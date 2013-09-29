#!/bin/bash

## Load includes
if [ "x$RADARGUN_HOME" = "x" ]; then DIRNAME=`dirname $0`; RADARGUN_HOME=`cd $DIRNAME/..; pwd` ; fi; export RADARGUN_HOME
. ${RADARGUN_HOME}/bin/includes.sh
. ${RADARGUN_HOME}/bin/environment.sh

CP=${RADARGUN_HOME}/lib/radargun-*.jar
JAVA="org.radargun.jmx.JmxRemoteOperation"
OP="unblock"
OBJ="Block"
HOST="localhost"
PORT=${JMX_MASTER_PORT}

help_and_exit() {
echo "usage: $0 [-host <hostname>] [-port <port>]"
exit 0;
}

while [ -n "$1" ]; do
case $1 in
  -host) HOST=$2; shift 2;;
  -port) PORT=$2; shift 2;;
  -h) help_and_exit;;
  -*) echo "Unknown option $1" shift 1;;
  *) echo "Unknown parameter $1" shift 1;;
esac
done

CMD="java -cp ${CP} ${JAVA} ${OBJ} ${OP} ${HOST} ${PORT}"
echo $CMD
eval $CMD
exit 0