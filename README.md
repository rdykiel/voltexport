README instructions for using voltexport tool
=============================================
This tool is designed to operate on a VoltDB 9.3.x installation.

It is used to read data from an export_overflow directory and output the data into .csv files in an output directory.

The included run.sh script allows building the tool, and running it. You must modify the run() function with your specific parameters as explained below.

Overview
--------

For each invocation of the run() function in run.sh, the tool will operate on one stream, and on one partition of that stream. By default, the export_overflow directory contains files for all the streams and all the partitions. On large configurations it may be advisable to separate the files for each stream and each partition in separate directories. Likewise, it may be desirable to separate the output files per stream and per partition in different directories.  

In the examples below we are using this separation principle to demonstrate the tool usage. The source directory contains all export_overflow files for stream EVENTS_TO_HBASE, partition 0. Note the file naming convention allowing identifying the stream and the partition in the file names:

    00-recovery/node2_events_to_hbase/0  
    ls -lh
    total 1642144
    -rw-r--r--@ 1 rdykiel  staff   2.6K Apr 17 01:08 EVENTS_TO_HBASE_0.ad
    -rw-r--r--@ 1 rdykiel  staff    64M Apr 17 07:38 EVENTS_TO_HBASE_0_0000000001_0000000002.pbd
    -rw-r--r--@ 1 rdykiel  staff    64M Apr 17 13:07 EVENTS_TO_HBASE_0_0000000003_0000000001.pbd
    -rw-r--r--@ 1 rdykiel  staff    64M Apr 18 01:52 EVENTS_TO_HBASE_0_0000000004_0000000003.pbd
    -rw-r--r--@ 1 rdykiel  staff    64M Apr 18 07:59 EVENTS_TO_HBASE_0_0000000005_0000000004.pbd
    -rw-r--r--@ 1 rdykiel  staff    64M Apr 18 13:36 EVENTS_TO_HBASE_0_0000000006_0000000005.pbd
    -rw-r--r--@ 1 rdykiel  staff    64M Apr 19 01:44 EVENTS_TO_HBASE_0_0000000007_0000000006.pbd
    -rw-r--r--@ 1 rdykiel  staff    63M Apr 19 02:49 EVENTS_TO_HBASE_0_0000000008_0000000007.pbd
    -rw-r--r--@ 1 rdykiel  staff    64M Apr 19 03:45 EVENTS_TO_HBASE_0_0000000009_0000000008.pbd
    -rw-r--r--@ 1 rdykiel  staff    64M Apr 19 05:01 EVENTS_TO_HBASE_0_0000000010_0000000009.pbd
    -rw-r--r--@ 1 rdykiel  staff    64M Apr 19 06:48 EVENTS_TO_HBASE_0_0000000011_0000000010.pbd
    -rw-r--r--@ 1 rdykiel  staff    64M Apr 19 08:29 EVENTS_TO_HBASE_0_0000000012_0000000011.pbd
    -rw-r--r--@ 1 rdykiel  staff    64M Apr 19 09:54 EVENTS_TO_HBASE_0_0000000013_0000000012.pbd
    -rw-r--r--@ 1 rdykiel  staff    36M Apr 19 11:36 EVENTS_TO_HBASE_0_0000000014_0000000013.pbd

IMPORTANT: voltexport operation is destructive
----------------------------------------------

The voltexport tool operates like VoltDB export, in that once the rows in a file have been completely exported to the csv file, the source ".pbd" file is deleted. At the end of the export operation, only the last file remains:

    -rw-r--r--@ 1 rdykiel  staff    36M Apr 19 11:36 EVENTS_TO_HBASE_0_0000000014_0000000013.pbd

Therefore it is important to **run the voltexport tool on copies of the original export_overflow files.**

Build the tool
--------------

The following command builds the voltexport tool, using the jars found in your VoltDB 9.3.x installation:

    ./run.sh jars

Modify the run() function parameters
------------------------------------

Edit run.sh to modify the following parameters used in the run() function to match your environment and the stream and partition you are exporting:

    function run() {
        java -classpath voltexport.jar:$APPCLASSPATH -Dlog4j.configuration=file:$LOG4J \
          org.voltdb.utils.voltexport.VoltExport \
          --indir=/Users/rdykiel/00-recovery/node2_events_to_hbase/0 \
          --outdir=/Users/rdykiel/00-recovery/out/node2/0 \
          --properties=FILE.properties \
          --stream_name=EVENTS_TO_HBASE \
          --partition=0
    }

Optionally modify the FILE.properties file
------------------------------------------

The FILE.properties files contains configuration options to the export client which cannot be specified via command-line parameters. Refer to the file export connector options described in your VoltDB documentation. By default FILE.properties contains the following options:

    cat FILE.properties
    skipinternals=true

For instance, you might decide you want to include the VoltDB metadata columns, in which case you would set **skipinternals=false**.

Invoke the run() command to export your rows
--------------------------------------------

With the data and setup described above, invoke the run() command to export your rows:

    ./run.sh run
    run
    2022-04-29 13:46:31,484 INFO: Detected stream EVENTS_TO_HBASE partition 0
    2022-04-29 13:46:31,486 INFO: Setting nonce to EVENTS_TO_HBASE_0 for FILE export
    2022-04-29 13:46:31,487 INFO: Exporting FILE to directory /Users/rdykiel/00-recovery/out/node2/0
    2022-04-29 13:46:31,595 INFO: Created export client org.voltdb.exportclient.ExportToFileClient
    2022-04-29 13:46:31,597 INFO: ExportRunner:EVENTS_TO_HBASE:0 exporting: skip = 0, count = 0
    WARN: Strict java memory checking is enabled, don't do release builds or performance runs with this enabled. Invoke "ant clean" and "ant -Djmemcheck=NO_MEMCHECK" to disable.
    WARN: Segment /Users/rdykiel/00-recovery/node2_events_to_hbase/0/EVENTS_TO_HBASE_0_0000000001_0000000002.pbd (final: false), has been recovered but is not in a final state
    WARN: Segment /Users/rdykiel/00-recovery/node2_events_to_hbase/0/EVENTS_TO_HBASE_0_0000000003_0000000001.pbd (final: false), has been recovered but is not in a final state
    WARN: Segment /Users/rdykiel/00-recovery/node2_events_to_hbase/0/EVENTS_TO_HBASE_0_0000000004_0000000003.pbd (final: false), has been recovered but is not in a final state
    WARN: Segment /Users/rdykiel/00-recovery/node2_events_to_hbase/0/EVENTS_TO_HBASE_0_0000000005_0000000004.pbd (final: false), has been recovered but is not in a final state
    WARN: Segment /Users/rdykiel/00-recovery/node2_events_to_hbase/0/EVENTS_TO_HBASE_0_0000000006_0000000005.pbd (final: false), has been recovered but is not in a final state
    WARN: Segment /Users/rdykiel/00-recovery/node2_events_to_hbase/0/EVENTS_TO_HBASE_0_0000000007_0000000006.pbd (final: false), has been recovered but is not in a final state
    WARN: Segment /Users/rdykiel/00-recovery/node2_events_to_hbase/0/EVENTS_TO_HBASE_0_0000000008_0000000007.pbd (final: false), has been recovered but is not in a final state
    WARN: Segment /Users/rdykiel/00-recovery/node2_events_to_hbase/0/EVENTS_TO_HBASE_0_0000000009_0000000008.pbd (final: false), has been recovered but is not in a final state
    WARN: Segment /Users/rdykiel/00-recovery/node2_events_to_hbase/0/EVENTS_TO_HBASE_0_0000000010_0000000009.pbd (final: false), has been recovered but is not in a final state
    WARN: Segment /Users/rdykiel/00-recovery/node2_events_to_hbase/0/EVENTS_TO_HBASE_0_0000000011_0000000010.pbd (final: false), has been recovered but is not in a final state
    WARN: Segment /Users/rdykiel/00-recovery/node2_events_to_hbase/0/EVENTS_TO_HBASE_0_0000000012_0000000011.pbd (final: false), has been recovered but is not in a final state
    WARN: Segment /Users/rdykiel/00-recovery/node2_events_to_hbase/0/EVENTS_TO_HBASE_0_0000000013_0000000012.pbd (final: false), has been recovered but is not in a final state
    WARN: Segment /Users/rdykiel/00-recovery/node2_events_to_hbase/0/EVENTS_TO_HBASE_0_0000000014_0000000013.pbd (final: false), has been recovered but is not in a final state
    2022-04-29 13:46:35,399 INFO: ExportRunner:EVENTS_TO_HBASE:0 scanned PBD: [355197412, 377778022]
    2022-04-29 13:47:21,164 INFO: ExportRunner:EVENTS_TO_HBASE:0 processed 22580611 rows (skipped = 0, exported = 22580611), export COMPLETE
    2022-04-29 13:47:21,166 INFO: Finished exporting stream EVENTS_TO_HBASE, partition 0

As explained above the operation was destructive of your source files:

    ls -lh node2_events_to_hbase/0
    total 73976
    -rw-r--r--@ 1 rdykiel  staff   2.6K Apr 17 01:08 EVENTS_TO_HBASE_0.ad
    -rw-r--r--@ 1 rdykiel  staff    36M Apr 19 11:36 EVENTS_TO_HBASE_0_0000000014_0000000013.pbd

The outdir contains the rows exported as a csv file:

    ls -lh out/node2/0
    total 5801464
    -rw-r--r--  1 rdykiel  staff   2.8G Apr 29 13:47 EVENTS_TO_HBASE_0-3765310264681971711-EVENTS_TO_HBASE-20220429174631.csv

Option --onlyscan
-----------------

Before actually exporting the rows, the run() command can be invoked with the **--onlyscan=true** option as follows:

    function run() {
        java -classpath voltexport.jar:$APPCLASSPATH -Dlog4j.configuration=file:$LOG4J \
        org.voltdb.utils.voltexport.VoltExport \
        --indir=/Users/rdykiel/00-recovery/node2_events_to_hbase/0 \
        --outdir=/Users/rdykiel/00-recovery/out/node2/0 \
        --properties=FILE.properties \
        --stream_name=EVENTS_TO_HBASE \
        --partition=0 \
        --onlyscan=true
    }

In this case, the tool execution stops after this line:

    2022-04-29 13:46:35,399 INFO: ExportRunner:EVENTS_TO_HBASE:0 scanned PBD: [355197412, 377778022]

No rows are exported, and no files are deleted. This option may be useful to determine the set of rows that the source files contain, and possibly to adjust the span of exported rows as explained below.

Options --skip and --count
--------------------------

These options are useful after a run with **--onlyscan=true** to select more precisely the span of rows that are exported. For instance, the example below will skip 150 rows at the beginning of the source files, and only export 500 rows from there:

    function run() {
        java -classpath voltexport.jar:$APPCLASSPATH -Dlog4j.configuration=file:$LOG4J \
        org.voltdb.utils.voltexport.VoltExport \
        --indir=/Users/rdykiel/00-recovery/node2_events_to_hbase/0 \
        --outdir=/Users/rdykiel/00-recovery/out/node2/0 \
        --properties=FILE.properties \
        --stream_name=EVENTS_TO_HBASE \
        --partition=0 \
        --onlyscan=true \
        --skip=150 \
        --count=500
    }

The parameters are independent. The default skip value is 0, meaning export from the first row, and the default value for count is 0, meaning export all the rows after the skipped ones.

Cleaning up the tool artifacts
------------------------------

The **clean** method allows removing all the artifacts created by the **jars** method:

    ./run.sh clean
    clean
