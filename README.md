README instructions for using voltexport tool
=============================================
This tool is designed to operate on a VoltDB 9.3.x installation.

It is used to read data from an export_overflow directory and output the data into .csv files in an output directory. The input and output directories may be the same.

The included run.sh script allows building the tool.

IMPORTANT: voltexport operation is destructive
----------------------------------------------

The voltexport tool operates like VoltDB export, in that once the rows in a file have been completely exported to the csv file, the source ".pbd" file is deleted, except if it is the last PBD file for this stream/partition. At the end of the export operation, only the last file PBD remains (but all its contents have been exported), e.g.:

    -rw-r--r--@ 1 rdykiel  staff    36M Apr 19 11:36 EVENTS_TO_HBASE_0_0000000014_0000000013.pbd

Therefore it is important to **run the voltexport tool on copies of the original export_overflow files, or have a backup of the original export_overflow files.**

Overview
--------

The main modes of operation of the tool are:

- **Scan**: Only display the sequence numbers found in the export overflow: this operation is not destructive of the original files.
- **Recover**: Perform the actual export: this operation deletes most of the original files and produces the resulting csv files.

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

Use the simple bash wrappers
----------------------------

The voltexport directory contains simple bash wrappers for the VoltExport tool:

- **scan**:         scan of 1 stream/partition
- **scanall**:      scan of all stream/partitions
- **recover**:      recover (export) of 1 stream/partition
- **recoverall**:   recover (export) of all stream/partitions

Enter the command name or invoke it with the **--help** option to see the possible parameters.

Select and modify the execution function parameters
---------------------------------------------------

The run.sh enables you to further customize the tool execution by creating a function with the exact parameters needed. See the implementation and the existing examples in run.sh.

Scan execution and output
-------------------------

The following are examples of **scan** or **scanall** invocations:

    ./scan --indir=/home/volt_dl_node1/voltdb_dl/voltdbroot/export_overflow --stream_name=EVENTS_TO_HDFS --partition=5
    ./scanall --indir=/home/volt_dl_node1/voltdb_dl/voltdbroot/export_overflow

The first example scans only stream EVENTS_TO_HDFS partition 5.

The second example scans all streams and partitions in the export_overflow.

The output of **scan** or **scanall** prints the range of sequence numbers of the rows present in the PBD files for each of the selected stream/partitions, e.g.:

    2022-10-12 15:27:35,624 INFO: ExportRunner:EVENTS_SUMMARY_DAY_TO_JDBC:5 scanned PBD: [25700250, 26340461]
    2022-10-12 15:27:35,751 INFO: ExportRunner:EVENTS_TO_HDFS:5 scanned PBD: [1, 11180]

The tool exits after displaying the scan results, without performing actual export nor modifying the original files. The scan output may also show 'gaps' in the sequence numbers.

Recover execution and output
----------------------------

The following are examples of **recover** or **recoverall** invocations:

    ./recover --indir=/home/volt_dl_node1/voltdb_dl/voltdbroot/export_overflow --stream_name=EVENTS_TO_HDFS --partition=5
    ./recoverall --indir-/home/volt_dl_node1/voltdb_dl/voltdbroot/export_overflow

The first example exports only stream EVENTS_TO_HDFS partition 5.

The second example exports all streams and partitions in the export_overflow.

The output of **recover** or **recoverall** prints the range of sequence numbers of the rows present in the PBD files, but also goes on exporting the selected stream/partitions. The successful export of a stream/partition produces an output like below:

    2022-10-12 15:28:54,287 INFO: ExportRunner:EVENTS_TO_HDFS:5 processed 11180 rows, export COMPLETE

In case errors are encountered in the export, that line would end with an **export INCOMPLETE** message.

Select rows to export with the --range option
---------------------------------------------

The **--range** options is useful after a scanning run with **scan** or **scanall** to select more precisely the span of rows that are exported. For instance, the example below will recover 202 rows: 101 rows before the gap, 101 rows after the gap:

    ./scan --indir=/tmp/demo1/node1/voltdbroot/export_overflow --stream_name=SOURCE003 --partition=1

    2022-10-19 09:06:16,681 INFO: Detected stream SOURCE003 partition 1
    2022-10-19 09:06:17,117 INFO: ExportRunner:SOURCE003:1 scanned PBD: [1, 41065] [54137, 76701] [82412, 89688]
    2022-10-19 09:06:17,118 INFO: Finished exporting stream SOURCE003, partition 1 in directory /tmp/demo1/node1/voltdbroot/export_overflow

    ./recover --indir=/tmp/demo1/node1/voltdbroot/export_overflow --stream_name=SOURCE003 --partition=1 --outdir=/tmp/demo1/out --range=76601,82512

    2022-10-19 09:06:26,088 INFO: Detected stream SOURCE003 partition 1
    2022-10-19 09:06:26,191 INFO: ExportRunner:SOURCE003:1 exporting range = [76601, 82512]
    2022-10-19 09:06:26,444 INFO: ExportRunner:SOURCE003:1 scanned PBD: [1, 41065] [54137, 76701] [82412, 89688]
    2022-10-19 09:06:26,537 INFO: ExportRunner:SOURCE003:1 exported 202 rows, export COMPLETE
    2022-10-19 09:06:26,538 INFO: Finished exporting stream SOURCE003, partition 1 in directory /tmp/demo1/node1/voltdbroot/export_overflow

The default range value is [0, 9223372036854775807], meaning export all the rows.

Cleaning up the tool artifacts
------------------------------

The **clean** method allows removing all the artifacts created by the **jars** method:

    ./run.sh clean
    clean
