#!/bin/bash

## Load includes
if [ "x$RADARGUN_HOME" = "x" ]; then DIRNAME=`dirname $0`; RADARGUN_HOME=`cd $DIRNAME/..; pwd` ; fi; export RADARGUN_HOME
. ${RADARGUN_HOME}/bin/includes.sh
. ${RADARGUN_HOME}/bin/environment.sh

CP=${RADARGUN_HOME}/lib/radargun-*.jar
JAVA="org.radargun.WorkloadJmxRequest"
OP="-high"
OBJ="TpccBenchmark"

help_and_exit() {
echo "usage: $0 -high|-low|-random -order <read only percentage> -order <write percentage> [-jmx-mbean <mbean name>] [-nr-threads <value>] <slaves list hostname:port>"
exit 0;
}

while [ -n "$1" ]; do
case $1 in
  -high) OP="-high"; shift 1;;
  -low) OP="-low"; shift 1;;
  -random) OP="-random"; shift 1;;
  -order) READ_PERCENT=$2; shift 2;;
  -payment) WRITE_PERCENT=$2; shift 2;;
  -jmx-mbean) OBJ=$2; shift 2;;
  -nr-threads) NR_THREADS="-nr-thread "$2; shift 2;;
  -h) help_and_exit;;
  -*) echo "Unknown option $1"; shift 1;;
  *) SLAVES=${SLAVES}" "$1; shift 1;;
esac
done

if [ -z "$SLAVES" ]; then
echo "No slaves found!";
help_and_exit;
fi

if [ -n "$WRITE_PERCENT" ]; then
WRITE_PERCENT="-payment-percent ${WRITE_PERCENT}";
fi

if [ -n "READ_PERCENT" ]; then
READ_PERCENT="-order-percent ${READ_PERCENT}";
fi

for slave in ${SLAVES}; do

if [[ "$slave" == *:* ]]; then
HOST="-hostname "`echo $slave | cut -d: -f1`
PORT="-port "`echo $slave | cut -d: -f2`
else
HOST="-hostname "$slave
PORT="-port "${JMX_SLAVES_PORT}
fi

CMD="java -cp ${CP} ${JAVA} ${OP} -jmx-component ${OBJ} ${WRITE_PERCENT} ${READ_PERCENT} ${NR_THREADS} ${HOST} ${PORT}"
echo $CMD
eval $CMD

done
exit 0