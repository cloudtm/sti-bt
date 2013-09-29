#!/bin/bash

##############
# Script to perform the population
#
# Author: Pedro Ruivo
#############

## Load includes
if [ "x$RADARGUN_HOME" = "x" ]; then DIRNAME=`dirname $0`; RADARGUN_HOME=`cd $DIRNAME/..; pwd` ; fi; export RADARGUN_HOME
. ${RADARGUN_HOME}/bin/includes.sh

CONFIG=./conf/tpcc-gen.properties

help_and_exit() {
  echo "Usage: "
  echo '  $ populate.sh [config]'
  echo ""
  echo "   [config]        Path to the framework configuration properties file. Optional - if not supplied benchmark will load ${CONFIG}"
  echo ""
  echo "   -h              show this message and exit"
  echo ""

  exit 0
}

welcome "This script is used to launch local benchmarks."

### read in any command-line params
while ! [ -z $1 ]
do
  case "$1" in
    "-h")
      help_and_exit
      ;;
    *)
      ;;
  esac
  shift
done

if [ -n "$1" ]; then
CONFIG=$1
fi

add_fwk_to_classpath
set_env
${JAVA} ${JVM_OPTS} -classpath $CP org.radargun.PopulateOnly ${CONFIG}
