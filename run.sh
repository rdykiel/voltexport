export VOLTDB_PATH=/Users/bballard/voltdb
export TOOL_PATH=$(pwd)
export CLASSPATH="$VOLTDB_PATH/voltdb/*:$VOLTDB_PATH/lib/*:$TOOL_PATH/*"
export EXPORT_OVERFLOW="/Users/bballard/instances/export/20201220_v9.3.2_export_overflow_recovery/backup_export_overflow"

echo "Running export data extraction tool with following paths:"
echo "  VOLTDB_PATH=$VOLTDB_PATH"
echo "  TOOL_PATH=$TOOL_PATH"
echo "  CLASSPATH=$CLASSPATH"
echo "  EXPORT_OVERFLOW=$EXPORT_OVERFLOW"

java -Dlog4j.configuration=file:$TOOL_PATH/log4j.properties org.voltdb.utils.voltexport.VoltExport \
     --export_overflow $EXPORT_OVERFLOW  --properties FILE.properties \
     --stream_name sessions \
     --partitions 1

# optional parameter:
#     --skip 0:1000,1:5000
