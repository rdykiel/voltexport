README instructions for using voltexport tool
=============================================
This tool is designed to operate on a VoltDB 9.3.x installation.

It is used to read data from an export_overflow directory and output the data into .csv files in an output directory. The input and output directories may be the same.

The included run.sh script allows building the tool, and running it. You must modify the available functions in run.sh with your specific parameters as explained below.

IMPORTANT: voltexport operation is destructive
----------------------------------------------

The voltexport tool operates like VoltDB export, in that once the rows in a file have been completely exported to the csv file, the source ".pbd" file is deleted, except if it is the last PBD file for this stream/partition. At the end of the export operation, only the last file PBD remains (but all its contents have been exported), e.g.:

    -rw-r--r--@ 1 rdykiel  staff    36M Apr 19 11:36 EVENTS_TO_HBASE_0_0000000014_0000000013.pbd

Therefore it is important to **run the voltexport tool on copies of the original export_overflow files, or have a backup of the original export_overflow files.**

Overview
--------

The main modes of operation of the tool are:

- **Export all streams/partitions**: Export all the stream/partitions found in the export_overflow directory (**--exportall=true**)
- **Export single stream/partition**: Export only 1 stream and 1 partition found in the export_overflow directory (**--exportall=false**): this is the default option.

For each of the above modes of operation, the **--onlyscan=true** option allows to scan the PBD files and print the range of sequence numbers of the stream/partition(s), without performing the actual export. In this case the operation is non-destructive. The option is **false** by default.

The --onlyscan option enables viewing the range of sequence numbers before performing the actual export:

- In the single stream/partition mode this enables select a precise range of rows to export, using the **--count** and **--skip** options.

Build the tool
--------------

The following command builds the voltexport tool, using the jars found in your VoltDB 9.3.x installation:

    ./run.sh jars

Optionally modify the FILE.properties file
------------------------------------------

The FILE.properties files contains configuration options to the export client which cannot be specified via command-line parameters. Refer to the file export connector options described in your VoltDB documentation. By default FILE.properties contains the following options:

    cat FILE.properties
    skipinternals=true

For instance, you might decide you want to include the VoltDB metadata columns, in which case you would set **skipinternals=false**.


Select and modify the execution function parameters
---------------------------------------------------

The run.sh contains 4 generic execution functions that you can either use as-is or modify with additional passed-in parameters:

- **scan()**: generic scan of 1 stream/partition
- **scanall()**: generic scan of all stream/partitions
- **recover()**: generic recover (export) of 1 stream/partition
- **recoverall()**: generic recover (export) of all stream/partitions

By default these functions read and write in the same directory (**--indir == --outdir**). You can modify the command to accept an additional parameter, e.g. specifying an **--outdir** different from **--indir**.

You can also create your own customized execution function.

Scan execution and output
-------------------------

The following are examples of **scan()** or **scanall()** invocations:

    ./run.sh scan /home/volt_dl_node1/voltdb_dl/voltdbroot/export_overflow EVENTS_TO_HDFS 5
    ./run.sh scanall /home/volt_dl_node1/voltdb_dl/voltdbroot/export_overflow

The first example scans only stream EVENTS_TO_HDFS partition 5.

The second example scans all streams and partitions in the export_overflow.

The output of **scan()** or **scanall()** prints the range of sequence numbers of the rows present in the PBD files for each of the selected stream/partitions, e.g.:

    2022-10-12 15:27:35,624 INFO: ExportRunner:EVENTS_SUMMARY_DAY_TO_JDBC:5 scanned PBD: [25700250, 26340461]
    2022-10-12 15:27:35,751 INFO: ExportRunner:EVENTS_TO_HDFS:5 scanned PBD: [1, 11180]

The tool exits after displaying the scan results, without performing actual export nor modifying the original files. The scan output may also show 'gaps' in the sequence numbers.

Recover execution and output
----------------------------

The following are examples of **recover()** or **recoverall()** invocations:

    ./run.sh recover /home/volt_dl_node1/voltdb_dl/voltdbroot/export_overflow EVENTS_TO_HDFS 5
    ./run.sh recoverall /home/volt_dl_node1/voltdb_dl/voltdbroot/export_overflow

The first example exports only stream EVENTS_TO_HDFS partition 5.

The second example exports all streams and partitions in the export_overflow.

The output of **recover()** or **recoverall()** prints the range of sequence numbers of the rows present in the PBD files, but also goes on exporting the selected stream/partitions. The successful export of a stream/partition produces an output like below:

    2022-10-12 15:28:54,287 INFO: ExportRunner:EVENTS_TO_HDFS:5 processed 11180 rows (skipped = 0, exported = 11180), export COMPLETE

In case errors are encountered in the export, that line would end with an **export INCOMPLETE** message.

Options --skip and --count
--------------------------

These options are useful after a run with **--onlyscan=true** to select more precisely the span of rows that are exported. For instance, the example below will skip 150 rows at the beginning of the source files, and only export 500 rows from there:

    function run() {
        java -classpath voltexport.jar:$APPCLASSPATH -Dlog4j.configuration=file:$LOG4J \
        org.voltdb.utils.voltexport.VoltExport \
        --indir=/home/volt_dl_node1/voltdb_dl/voltdbroot/export_overflow \
        --outdir=/home/volt_dl_node1/voltdb_dl/voltdbroot/export_overflow \
        --properties=FILE.properties \
        --stream_name=EVENTS_TO_HBASE \
        --partition=0 \
        --onlyscan=false \
        --skip=150 \
        --count=500
    }

The parameters are independent. The default skip value is 0, meaning export from the first row, and the default value for count is 0, meaning export all the rows after the skipped ones.

Cleaning up the tool artifacts
------------------------------

The **clean** method allows removing all the artifacts created by the **jars** method:

    ./run.sh clean
    clean
