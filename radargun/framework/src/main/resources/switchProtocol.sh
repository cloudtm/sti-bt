#!/bin/bash

## Load includes
if [ "x$RADARGUN_HOME" = "x" ]; then DIRNAME=`dirname $0`; RADARGUN_HOME=`cd $DIRNAME/..; pwd` ; fi; export RADARGUN_HOME
. ${RADARGUN_HOME}/bin/includes.sh
. ${RADARGUN_HOME}/bin/environment.sh

CP=${RADARGUN_HOME}/lib/radargun-*.jar
JAVA="org.radargun.SwitchJmxRequest"
OBJ="ReconfigurableReplicationManager"

help_and_exit() {
echo "usage: $0 <slave> [-print-stats] [-print-state] -protocol <protocol id> [-force-stop] [-jmx-mbean <mbean name>]"
echo "   slave: <hostname or hostname:port>"
exit 0;
}

if [ -n "$1" ]; then
slave=$1;
fi

while [ -n "$1" ]; do
case $1 in
  -protocol) PROTOCOL=$2; shift 2;;
  -force-stop) FORCE_STOP="-force-stop"; shift 1;;  
  -jmx-mbean) OBJ=$2; shift 2;;
  -print-stats) PRINT_STATS="true"; shift 1;;
  -print-state) PRINT_STATE="true"; shift 1;;
  -h) help_and_exit;;
  -*) echo "Unknown option $1"; shift 1;;
  *) SLAVE=$1; shift 1;;
esac
done

if [ -z "$SLAVE" ]; then
echo "Slave not found!";
help_and_exit;
fi

if [[ "$SLAVE" == *:* ]]; then
HOST=`echo $SLAVE | cut -d: -f1`
PORT=`echo $SLAVE | cut -d: -f2`
else
HOST=$SLAVE
PORT=${JMX_SLAVES_PORT}
fi

if [ -n "$PRINT_STATS" ]; then
CMD="java -cp ${CP} ${JAVA} -jmx-component ${OBJ} -print-stats -hostname ${HOST} -port ${PORT}"
echo $CMD
eval $CMD
exit 0
fi

if [ -n "$PRINT_STATE" ]; then
CMD="java -cp ${CP} ${JAVA} -jmx-component ${OBJ} -print-state -hostname ${HOST} -port ${PORT}"
echo $CMD
eval $CMD
exit 0
fi

if [ -z "$PROTOCOL" ]; then
echo "Protocol is required";
help_and_exit;
fi

CMD="java -cp ${CP} ${JAVA} -jmx-component ${OBJ} -protocol ${PROTOCOL} -hostname ${HOST} -port ${PORT} ${FORCE_STOP}"
echo $CMD
eval $CMD


exit 0
