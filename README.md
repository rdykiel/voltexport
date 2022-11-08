README instructions for using voltexport tools
==============================================
This set of tools is used to read data from an export_overflow directory and output the data into .csv files in an output directory.

The included run.sh script allows building the tools.

IMPORTANT: voltexport operation is destructive
----------------------------------------------

The voltexport tools operates like VoltDB export, in that once the rows in a file have been completely exported to the csv file, the source ".pbd" file is deleted, except if it is the last PBD file for this stream/partition. At the end of an export operation, only the last file PBD remains (but all its contents have been exported), e.g.:

    -rw-r--r--@ 1 rdykiel  staff    36M Apr 19 11:36 EVENTS_TO_HBASE_0_0000000014_0000000013.pbd

Therefore it is important to **run the voltexport tools on copies of the original export_overflow files, or have a backup of the original export_overflow files.**

Overview
--------

The main use cases of the tools are:

- **Scan**: Only display the sequence numbers found in one export overflow directory: this operation is not destructive of the original files.
- **Recover**: Export the files found in one export overflow directory: this operation deletes most of the original files and produces the resulting csv files.
- **Stitch**: Reconstruct an export stream and export it, from multiple export overflow directories, filling gaps when the different nodes are missing export rows.

The main parameters for these tools are:

- **indir**:  Full path of export_overflow directory (or a copy of)
- **outdir**: Full path of output directory (optional for scan or recover, mandatory for stitch)
- **catalog**: Full path of catalog jar file: this is a required parameter
- **stream_name**: the stream or table name when scanning, recovering, or stitching a particular table or stream
- **partition**: the partition to scan, recover, or stitch

Developers: build VoltDB
------------------------

In case a developer wants to run the tools, VoltDB must be built without memory checking (a production build is ideal), because the handling of ranges may leak memory in the tool (i.e. not discarding the last polled buffer): this is done in order to avoid deleting the PBD file in case it is no polled completely.

Build the tools
---------------

The following command builds the voltexport tools, using the jars found in your VoltDB installation:

    ./run.sh jars

Optionally modify the FILE.properties file
------------------------------------------

The FILE.properties files contains configuration options to the export client which cannot be specified via command-line parameters. Refer to the file export connector options described in your VoltDB documentation. By default FILE.properties contains the following options:

    cat FILE.properties
    skipinternals=true

For instance, you might decide you want to include the VoltDB metadata columns, in which case you would set **skipinternals=false**.

Use the simple bash wrappers
----------------------------

The voltexport directory contains simple bash wrappers for the VoltExport tools:

- **scan**:         scan of 1 stream/partition
- **scanall**:      scan of all stream/partitions
- **recover**:      recover (export) of 1 stream/partition
- **recoverall**:   recover (export) of all stream/partitions
- **stitch**:       reconstruct and export 1 stream/partition from multiple export_overflow directories

Enter the command name or invoke it with the **--help** option to see the possible parameters.

Select and modify the execution function parameters
---------------------------------------------------

The run.sh enables you to further customize the tool execution by creating a function with the exact parameters needed. See the implementation and the existing examples in run.sh.

Scan execution and output
-------------------------

The following are examples of **scan** or **scanall** invocations:

    ./scan --indir=/home/volt_dl_node1/voltdb_dl/voltdbroot/export_overflow --stream_name=EVENTS_TO_HDFS --partition=5 --catalog=/home/volt_dl_node1/voltdb_dl/voltdbroot/config/catalog.jar
    ./scanall --indir=/home/volt_dl_node1/voltdb_dl/voltdbroot/export_overflow --catalog=/home/volt_dl_node1/voltdb_dl/voltdbroot/config/catalog.jar

The first example scans only stream EVENTS_TO_HDFS partition 5.

The second example scans all streams and partitions in the export_overflow.

The output of **scan** or **scanall** prints the range of sequence numbers of the rows present in the PBD files for each of the selected stream/partitions, e.g.:

    2022-10-12 15:27:35,624 INFO: ExportRunner:EVENTS_SUMMARY_DAY_TO_JDBC:5 scanned PBD: [25700250, 26340461]
    2022-10-12 15:27:35,751 INFO: ExportRunner:EVENTS_TO_HDFS:5 scanned PBD: [1, 11180]

The tool exits after displaying the scan results, without performing actual export nor modifying the original files. The scan output may also show 'gaps' in the sequence numbers.

Recover execution and output
----------------------------

The following are examples of **recover** or **recoverall** invocations:

    ./recover --indir=/home/volt_dl_node1/voltdb_dl/voltdbroot/export_overflow --stream_name=EVENTS_TO_HDFS --partition=5 --catalog=/home/volt_dl_node1/voltdb_dl/voltdbroot/config/catalog.jar
    ./recoverall --indir-/home/volt_dl_node1/voltdb_dl/voltdbroot/export_overflow --catalog=/home/volt_dl_node1/voltdb_dl/voltdbroot/config/catalog.jar

The first example exports only stream EVENTS_TO_HDFS partition 5.

The second example exports all streams and partitions in the export_overflow.

The output of **recover** or **recoverall** prints the range of sequence numbers of the rows present in the PBD files, but also goes on exporting the selected stream/partitions. The successful export of a stream/partition produces an output like below:

    2022-10-12 15:28:54,287 INFO: ExportRunner:EVENTS_TO_HDFS:5 processed 11180 rows, export COMPLETE

In case errors are encountered in the export, that line would end with an **export INCOMPLETE** message.

Select rows to export with the --range option
---------------------------------------------

The **--range** options is useful after a scanning run with **scan** or **scanall** to select more precisely the span of rows that are exported. For instance, the example below will recover 202 rows: 101 rows before the gap, 101 rows after the gap:

    ./scan --indir=/tmp/demo1/node1/voltdbroot/export_overflow --stream_name=SOURCE003 --partition=1 --catalog=/tmp/demo1/node1/voltdbroot/config/catalog.jar

    2022-10-19 09:06:16,681 INFO: Detected stream SOURCE003 partition 1
    2022-10-19 09:06:17,117 INFO: ExportRunner:SOURCE003:1 scanned PBD: [1, 41065] [54137, 76701] [82412, 89688]
    2022-10-19 09:06:17,118 INFO: Finished exporting stream SOURCE003, partition 1 in directory /tmp/demo1/node1/voltdbroot/export_overflow

    ./recover --indir=/tmp/demo1/node1/voltdbroot/export_overflow --stream_name=SOURCE003 --partition=1 --outdir=/tmp/demo1/out --range=76601,82512  --catalog=/tmp/demo1/node1/voltdbroot/config/catalog.jar

    2022-10-19 09:06:26,088 INFO: Detected stream SOURCE003 partition 1
    2022-10-19 09:06:26,191 INFO: ExportRunner:SOURCE003:1 exporting range = [76601, 82512]
    2022-10-19 09:06:26,444 INFO: ExportRunner:SOURCE003:1 scanned PBD: [1, 41065] [54137, 76701] [82412, 89688]
    2022-10-19 09:06:26,537 INFO: ExportRunner:SOURCE003:1 exported 202 rows, export COMPLETE
    2022-10-19 09:06:26,538 INFO: Finished exporting stream SOURCE003, partition 1 in directory /tmp/demo1/node1/voltdbroot/export_overflow

The default range value is [0, 9223372036854775807], meaning export all the rows.

Stitch: reconstruct an export stream from multiple export overflow directories
------------------------------------------------------------------------------

In a K >= 1 multi-node cluster, nodes can go down and rejoin while applications are running. Therefore the streams in the export overflow of these nodes may miss some export rows: these missing rows are called 'gaps'. For instance in the example below, the stream/partition has 2 gaps, **[35122, 46499]**, and **[65901, 70858]**:

    022-10-20 10:53:01,243 INFO: ExportRunner:SOURCE003:3 scanned PBD: [1, 35121] [46500, 65900] [70859, 77031]

Thanks to the k-factor mechanism, the missing rows may be present in the export overflow directory of another node. Using the **scan** and **recover** tools it is possible to manually reconstruct an export of all the rows, by exporting selected ranges from each but this is tedious and error-prone.

The **stitch** tool allows reconstructing a stream/partition from multiple export overflow directories and export the results in an output directory.

**Note**: in versions above 9.3.x, the likelihood of gaps in the export rows is reduced thanks to a background mechanism copying the missing rows from the other nodes when rejoining the cluster. Therefore using stitch may not be needed as it is on 9.3.x.

In the example below we stitch the stream SOURCE003 partition 3 from 3 export overflow directories:

    ./stitch --indirs=/tmp/demo2/node0/voltdbroot/export_overflow,/tmp/demo2/node1/voltdbroot/export_overflow,/tmp/demo2/node2/voltdbroot/export_overflow \
      --outdir=/tmp/demo2/out --stream_name=SOURCE003 --partition=3 \
      --catalog=/tmp/demo2/node0/voltdbroot/config/catalog.jar

    2022-10-20 10:53:00,872 INFO: Host 0: /tmp/demo2/node0/voltdbroot/export_overflow
    2022-10-20 10:53:00,872 INFO: Host 1: /tmp/demo2/node1/voltdbroot/export_overflow
    2022-10-20 10:53:00,872 INFO: Host 2: /tmp/demo2/node2/voltdbroot/export_overflow
    2022-10-20 10:53:01,163 INFO: ExportRunner:SOURCE003:3 scanned PBD: Empty Map
    2022-10-20 10:53:01,243 INFO: ExportRunner:SOURCE003:3 scanned PBD: [1, 35121] [46500, 65900] [70859, 77031]
    2022-10-20 10:53:01,264 INFO: ExportRunner:SOURCE003:3 scanned PBD: [1, 52474] [57779, 77031]
    2022-10-20 10:53:01,266 INFO: Host 0: null
    2022-10-20 10:53:01,266 INFO: Host 1: [1, 35121] [46500, 65900] [70859, 77031]
    2022-10-20 10:53:01,266 INFO: Host 2: [1, 52474] [57779, 77031]
    2022-10-20 10:53:01,267 INFO: Host 1 mastership: [1, 35121] [46500, 65900] [70859, 77031]
    2022-10-20 10:53:01,268 INFO: Host 2 mastership: [35122, 46499] [65901, 70858]
    2022-10-20 10:53:01,269 INFO: Starting 2 segments runners for a total of 77031 rows to export ...
    2022-10-20 10:53:01,269 INFO: Running export runner for host 1 range [1, 35121]
    2022-10-20 10:53:01,269 INFO: Running export runner for host 2 range [35122, 46499]
    2022-10-20 10:53:01,270 INFO: ExportRunner:SOURCE003:3 exporting range = [1, 35121]
    2022-10-20 10:53:01,270 INFO: ExportRunner:SOURCE003:3 exporting range = [35122, 46499]
    2022-10-20 10:53:01,288 INFO: ExportRunner:SOURCE003:3 scanned PBD: [1, 35121] [46500, 65900] [70859, 77031]
    2022-10-20 10:53:01,290 INFO: ExportRunner:SOURCE003:3 scanned PBD: [1, 52474] [57779, 77031]
    2022-10-20 10:53:01,515 INFO: ExportRunner:SOURCE003:3 exported 11378 rows, export COMPLETE
    2022-10-20 10:53:01,515 INFO: Running export runner for host 2 range [65901, 70858]
    2022-10-20 10:53:01,516 INFO: ExportRunner:SOURCE003:3 exporting range = [65901, 70858]
    2022-10-20 10:53:01,528 INFO: ExportRunner:SOURCE003:3 scanned PBD: [42318, 52474] [57779, 77031]
    2022-10-20 10:53:01,581 INFO: ExportRunner:SOURCE003:3 exported 4958 rows, export COMPLETE
    2022-10-20 10:53:01,745 INFO: ExportRunner:SOURCE003:3 exported 35121 rows, export COMPLETE
    2022-10-20 10:53:01,746 INFO: Running export runner for host 1 range [46500, 65900]
    2022-10-20 10:53:01,746 INFO: ExportRunner:SOURCE003:3 exporting range = [46500, 65900]
    2022-10-20 10:53:01,756 INFO: ExportRunner:SOURCE003:3 scanned PBD: [32333, 35121] [46500, 65900] [70859, 77031]
    2022-10-20 10:53:01,885 INFO: ExportRunner:SOURCE003:3 exported 19401 rows, export COMPLETE
    2022-10-20 10:53:01,885 INFO: Running export runner for host 1 range [70859, 77031]
    2022-10-20 10:53:01,885 INFO: ExportRunner:SOURCE003:3 exporting range = [70859, 77031]
    2022-10-20 10:53:01,890 INFO: ExportRunner:SOURCE003:3 scanned PBD: [61753, 65900] [70859, 77031]
    2022-10-20 10:53:01,936 INFO: ExportRunner:SOURCE003:3 exported 6173 rows, export COMPLETE
    2022-10-20 10:53:01,940 INFO: Waiting for 2 segments runner completions ...
    2022-10-20 10:53:01,941 INFO: Success: stitching 77031 rows of SOURCE003:3 COMPLETE

The tool starts by scanning the 3 export overflow directories and displays the sequence numbers of the rows it found in each of the input directories:

    2022-10-20 10:53:01,266 INFO: Host 0: null
    2022-10-20 10:53:01,266 INFO: Host 1: [1, 35121] [46500, 65900] [70859, 77031]
    2022-10-20 10:53:01,266 INFO: Host 2: [1, 52474] [57779, 77031]

Next, the tool computes which rows it will export from each node:

    2022-10-20 10:53:01,267 INFO: Host 1 mastership: [1, 35121] [46500, 65900] [70859, 77031]
    2022-10-20 10:53:01,268 INFO: Host 2 mastership: [35122, 46499] [65901, 70858]

Finally, the tool performs the actual export: this is done in parallel for the rows of each node. A message with **COMPLETE** indicates the successful export of each 'segment' of rows selected. The final message indicates the total number of rows exported:

    2022-10-20 10:53:01,941 INFO: Success: stitching 77031 rows of SOURCE003:3 COMPLETE

The resulting csv files have a name convention ensuring that the exported rows are properly sequenced in order when the files are listed in the lexical order of the file names:

    wc -l out/*
      27326 out/SOURCE003_3_1_35121-3916392722566529023-SOURCE003-20221020145301.csv
      5006 out/SOURCE003_3_1_35121-3916394074633994239-SOURCE003-20221020145301.csv
      2789 out/SOURCE003_3_1_35121-3916394326762012671-SOURCE003-20221020145301.csv
      7196 out/SOURCE003_3_35122_46499-3916394326762012671-SOURCE003-20221020145301.csv
      4182 out/SOURCE003_3_35122_46499-3916394820884594687-SOURCE003-20221020145301.csv
      8923 out/SOURCE003_3_46500_65900-3916394820884594687-SOURCE003-20221020145301.csv
      6330 out/SOURCE003_3_46500_65900-3916395469173014527-SOURCE003-20221020145301.csv
      4148 out/SOURCE003_3_46500_65900-3916395784442085375-SOURCE003-20221020145301.csv
      2300 out/SOURCE003_3_65901_70858-3916395784442085375-SOURCE003-20221020145301.csv
      2658 out/SOURCE003_3_65901_70858-3916396114701598719-SOURCE003-20221020145301.csv
      6173 out/SOURCE003_3_70859_77031-3916396114701598719-SOURCE003-20221020145301.csv
    77031 total

For instance, the following files contain the rows from the **[35122, 46499]** exported from Host 2:

    7196 out/SOURCE003_3_35122_46499-3916394326762012671-SOURCE003-20221020145301.csv
    4182 out/SOURCE003_3_35122_46499-3916394820884594687-SOURCE003-20221020145301.csv

The reason there are more than one file for that range is that this range of rows were stored in 2 different PBD files (new PBD files are created when they either reach the 64Mb limit, or a catalog update occurred). The naming convention of the csv files ensures that the rows can be taken in the correct order.

Cleaning up the tool artifacts
------------------------------

The **clean** method allows removing all the artifacts created by the **jars** method:

    ./run.sh clean
    clean
