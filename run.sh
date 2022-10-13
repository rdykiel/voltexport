#!/usr/bin/env bash

APPNAME="voltexport"

# find voltdb binaries in either installation or distribution directory.
if [ -n "$(which voltdb 2> /dev/null)" ]; then
    VOLTDB_BIN=$(dirname "$(which voltdb)")
else
    VOLTDB_BIN="$(dirname $(dirname $(dirname $(pwd))))/bin"
    echo "The VoltDB scripts are not in your PATH."
    echo "For ease of use, add the VoltDB bin directory: "
    echo
    echo $VOLTDB_BIN
    echo
    echo "to your PATH."
    echo
fi

# call script to set up paths, including
# java classpaths and binary paths
source $VOLTDB_BIN/voltenv

VOLTDB="$VOLTDB_BIN/voltdb"
LOG4J="$VOLTDB_VOLTDB/log4j.xml"
HOST="localhost"

# Grab the necessary command line arguments
function parse_command_line() {
    OPTIND=1
    # Return the function to run
    shift $(($OPTIND - 1))
    RUN=$@
}

# remove build artifacts
function clean() {
    rm -rf obj voltdbroot log *.jar *.csv
    find . -name '*.class' | xargs rm -f
}

function jars() {
    echo
    echo "Compile server APPCLASSPATH=\"${APPCLASSPATH}\""
    echo
    javac -classpath $APPCLASSPATH src/org/voltdb/utils/voltexport/*.java
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi
    jar cf voltexport.jar -C src org
}

# Generic scan 1 stream/partition
# Usage: ./run.sh scan full_path_in_out_dir stream_name partition
function scan() {
  java -classpath voltexport.jar:$APPCLASSPATH -Dlog4j.configuration=file:$LOG4J \
      org.voltdb.utils.voltexport.VoltExport \
      --indir=$1 \
      --outdir=$1 \
      --properties=FILE.properties \
      --stream_name=$2 \
      --partition=$3 \
      --onlyscan=true
}

# Generic scan all stream/partition, recovers in same directory
# Usage: ./run.sh scanall full_path_in_out_dir
function scanall() {
  java -classpath voltexport.jar:$APPCLASSPATH -Dlog4j.configuration=file:$LOG4J \
      org.voltdb.utils.voltexport.VoltExport \
      --indir=$1 \
      --outdir=$1 \
      --properties=FILE.properties \
      --exportall=true \
      --onlyscan=true
}

# Generic revover 1 stream/partition
# Usage: ./run.sh recover full_path_in_out_dir stream_name partition
function recover() {
  java -classpath voltexport.jar:$APPCLASSPATH -Dlog4j.configuration=file:$LOG4J \
      org.voltdb.utils.voltexport.VoltExport \
      --indir=$1 \
      --outdir=$1 \
      --properties=FILE.properties \
      --stream_name=$2 \
      --partition=$3 \
      --skip=$4
}

# Generic recover all stream/partition, recovers in same directory
# Usage: ./run.sh recoverall full_path_in_out_dir
function recoverall() {
  java -classpath voltexport.jar:$APPCLASSPATH -Dlog4j.configuration=file:$LOG4J \
      org.voltdb.utils.voltexport.VoltExport \
      --indir=$1 \
      --outdir=$1 \
      --properties=FILE.properties \
      --exportall=true
}

function help() {
echo "
Usage: run.sh TARGET
Targets:
    clean
    jars | jars-ifneeded | servercompile | clientcompile
    server | init
    client
"
}

parse_command_line $@
if [ -n "$RUN" ]; then
    echo $RUN
    $RUN
else
    help
fi
