README instructions for using voltdb export tool
================================================
This tool is designed to operate on a VoltDB 9.3.x installation.

It is used to read data from an export_overflow directory and output the data into .csv files in an output directory.

To run the tool, you can use the included run.sh script or it can be invoked manually as a java command from the command line.


Option 1: using the script
--------------------------

a) First, edit the run.sh script to set the correct path variables and command-line parameter values.

    export VOLTDB_PATH=/opt/voltdb-ent-9.3.2
    export TOOL_PATH=$(pwd)
    export CLASSPATH="$VOLTDB_PATH/voltdb/*:$VOLTDB_PATH/lib/*:$TOOL_PATH/*"
    export EXPORT_OVERFLOW="/data/export_overflow"


b) Next, edit the FILE.properties file. Set the nonce property to the prefix name you want the output files to use, and set the outdir property to the path of the folder where you want the output to go. The folder will be created automatically if it doesn't already exist.

    nonce=MYDATA
    outdir=csv_output
    skipinternals=true

The nonce is a prefix for the output file. The outdir is the directory for the output. Skipinternals will omit additional metadata columns from the csv output that are not part of the stream definition, so this should be set to true for loading the data into another system.

c) Then run the script like this:

    ./run.sh


Option 2: manually from the command line
----------------------------------------

a) edit the FILE.properties file as described above

b) You must set up a CLASSPATH to the VoltDB distribution and ending with the directory where the tool is installed

    export CLASSPATH=/opt/voltdb-ent-9.3.2/voltdb/*:/opt/voltdb-ent-9.3.2/lib/*:/opt/voltdb_export_tool/*

c) run the tool from the command line, setting the correct log4j.properties path and export_overflow path

    java -Dlog4j.configuration=file:/opt/voltdb_export_tool/log4j.properties org.voltdb.utils.voltexport.VoltExport --export_overflow /data/export_overflow --properties FILE.properties --stream_name MY_STREAM

The following parameters can be used:

    usage: org.voltdb.utils.voltexport.VoltExport
        --export_overflow <arg>   export_overflow directory (or location of
                                  saved export files)
        --partitions <arg>        partitions to export (comma-separated,
                                  default all partitions)
        --properties <arg>        Properties file or a string which can be
                                  parsed as a properties file, for export
                                  target configuration
        --skip <arg>              list of skip entries allowing skipping rows
                                  to export (comma-separated, default no
                                  skipping): each list element as 'X:Y'
                                  (X=partition, Y=count)
        --stream_name <arg>       stream name to export

It's better to run the tool from the installation (voltdb_export_tool) directory - the full path to the log4j properties must stil be provided, but the properties file can use just the filename if it's in the current directory.

Expected output
---------------

The tool will output something like this:

    INFO: Detected stream SESSIONS partition 0
    INFO: Detected stream SESSIONS partition 1
    ...
    INFO: Exporting FILE to directory /opt/voltdb_export_tool/csv_output
    INFO: Loading VoltDB native library catalog-9.3.4 from the system library location. A confirmation message will follow if the loading is successful.
    INFO: Retry loading from file.
    INFO: Loading VoltDB native library catalog-9.3.4 from file /var/folders/b0/dt55h2p96ml_pfjn2f8j30tw0000gn/T/voltdb-8469ff93-f88f-4695-870e-b81817da1e86-libcatalog-9.3.4.jnilib. A confirmation message will follow if the loading is successful.
    INFO: Successfully loaded VoltDB native library catalog-9.3.4.
    INFO: Loading VoltDB native library voltdb-9.3.4 from file /var/folders/b0/dt55h2p96ml_pfjn2f8j30tw0000gn/T/voltdb-037805f9-a595-42ae-8f55-a6dfcb950250-libvoltdb-9.3.4.jnilib. A confirmation message will follow if the loading is successful.
    INFO: Successfully loaded VoltDB native library voltdb-9.3.4.
    WARN: Segment /data/export_overflow/SESSIONS_0_0000000001_0000000002.pbd (final: false), has been recovered but is not in a final state
    WARN: Segment /data/export_overflow/SESSIONS_0_0000000003_0000000001.pbd (final: false), has been recovered but is not in a final state
    ...
    INFO: ExportRunner:sessions:0 processed 104835376 rows, export COMPLETE
    WARN: Segment /data/export_overflow/SESSIONS_1_0000000001_0000000002.pbd (final: false), has been recovered but is not in a final state
    WARN: Segment /data/export_overflow/SESSIONS_1_0000000003_0000000001.pbd (final: false), has been recovered but is not in a final state
    ...
    INFO: ExportRunner:sessions:1 processed 104475438 rows, export COMPLETE
    INFO: Finished exporting 2 partitions of stream sessions

In the specified outdir directory, there will be one file output for every stream, for every time you run the tool. While the file is being written it will have an active-* prefix:

    active-MYDATA-3434525720150310911-SESSIONS-20201221222610.csv

Once it has been completed the active-* prefix will be removed:

    MYDATA-3434525720150310911-SESSIONS-20201221222610.csv

Important note about partitions (and extracting one partition at a time)
----------------------------------------------------------------------------

If you have a copy of the export_overflow directory from each of the servers in a cluster, it will contain one or more files for each stream + partition combination. For example, here we see the SESSIONS stream has partitions 0 and 1:

    -rw-r--r--   1 bballard  staff      1088 Dec 21 13:32 SESSIONS_0.ad
    -rw-r--r--   1 bballard  staff  65203674 Dec 21 13:32 SESSIONS_0_0000000001_0000000002.pbd
    -rw-r--r--   1 bballard  staff  64787717 Dec 21 13:32 SESSIONS_0_0000000003_0000000001.pbd
    -rw-r--r--   1 bballard  staff  65225620 Dec 21 13:32 SESSIONS_0_0000000004_0000000003.pbd
    -rw-r--r--   1 bballard  staff      1088 Dec 21 13:32 SESSIONS_1.ad
    -rw-r--r--   1 bballard  staff  65117629 Dec 21 13:32 SESSIONS_1_0000000001_0000000002.pbd
    -rw-r--r--   1 bballard  staff  64662425 Dec 21 13:32 SESSIONS_1_0000000003_0000000001.pbd
    -rw-r--r--   1 bballard  staff  64906460 Dec 21 13:32 SESSIONS_1_0000000004_0000000003.pbd

The export_overflow folders from other servers in the cluster will have another copy of these partitions. Since the tool will mix data from multiple partitions into one output file for each stream, it is recommended to use the optional --partitions parameter and use the nonce property in FILE.properties to change the prefix for each partition. Then you can keep track of the files for each partition of each stream and ensure when the csv files are loaded you only load the data from each partition once.

For example, in FILE.properties:

    nonce=PARTITION_0
    outdir=csv_output
    skipinternals=true

In run.sh:

    ...
    --stream_name sessions \
    --partitions 0

resulting output:

    csv_output/PARTITION_0-3434525720150310911-SESSIONS-20201221222610.csv


You can also specify a comma-separated list of partitions for the --partitions parameter, and keep track of which partitions you are extracting from each server's export_overflow directory, and be sure not to duplicate them.


If you run out of disk space and need to start over at some point, you could count the # of rows output from the prior run and then skip that number of rows in the next run of the tool by setting the --skip parameter.
