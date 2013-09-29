#!/bin/bash

## Load includes
if [ "x$RADARGUN_HOME" = "x" ]; then DIRNAME=`dirname $0`; RADARGUN_HOME=`cd $DIRNAME/..; pwd` ; fi; export RADARGUN_HOME
. ${RADARGUN_HOME}/bin/includes.sh
. ${RADARGUN_HOME}/bin/environment.sh

CP=${RADARGUN_HOME}/lib/radargun-*.jar
JAVA="org.radargun.StopJmxRequest"
OBJ="TpccBenchmark"

help_and_exit() {
echo "usage: $0 [-jmx-mbean <mbean name>] <slaves list hostname:port>"
exit 0;
}

while [ -n "$1" ]; do
case $1 in  
  -jmx-mbean) OBJ=$2; shift 2;;  
  -h) help_and_exit;;
  -*) echo "Unknown option $1"; shift 1;;
  *) SLAVES=${SLAVES}" "$1; shift 1;;
esac
done

if [ -z "$SLAVES" ]; then
echo "No slaves found!";
help_and_exit;
fi

for slave in ${SLAVES}; do

if [[ "$slave" == *:* ]]; then
HOST="-hostname "`echo $slave | cut -d: -f1`
PORT="-port "`echo $slave | cut -d: -f2`
else
HOST="-hostname "$slave
PORT="-port "${JMX_SLAVES_PORT}
fi

CMD="java -cp ${CP} ${JAVA} -jmx-component ${OBJ} ${HOST} ${PORT}"
echo $CMD
eval $CMD

done
exit 0