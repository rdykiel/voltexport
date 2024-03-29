/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.voltdb.utils.voltexport;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.commons.lang3.StringUtils;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltcore.utils.Pair;
import org.voltdb.CLIConfig;
import org.voltdb.CatalogContext;
import org.voltdb.ClientInterface;
import org.voltdb.ExportStatsBase.ExportStatsRow;
import org.voltdb.SnapshotCompletionMonitor.ExportSnapshotTuple;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Database;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.export.ExportDataSource.StreamStartAction;
import org.voltdb.export.ExportManagerInterface;
import org.voltdb.export.ExportStats;
import org.voltdb.export.Generation;
import org.voltdb.export.StreamControlOperation;
import org.voltdb.exportclient.ExportClientBase;
import org.voltdb.exportclient.ExportToFileClient;
import org.voltdb.exportclient.JDBCExportClient;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.InMemoryJarfile;
import org.voltdb.utils.StringInputStream;

public class VoltExport {
    public static final MyLogger LOG = new MyLogger();
    public static final VoltLogger VOLTLOG = new VoltLogger("VOLTEXPORT");

    /**
     * Configuration options
     */
    public static class VoltExportConfig extends CLIConfig implements Cloneable {

        @Option(desc = "input directory, either export_overflow, or location of saved export files")
        String indir = "";

        @Option(desc = "output directory for file export (may be omitted if onlyscan = true or exportall is false")
        String outdir = "";

        @Option(desc = "catalog file - must be provided")
        String catalog = "";

        @Option(desc = "Properties file or a string which can be parsed as a properties file, for export target configuration")
        String properties = "";

        @Option(desc = "export all streams/partitions in indir (default false)")
        boolean exportall = false;

        @Option(desc = "stream name to export, ignored if exportall")
        String stream_name = "";

        @Option(desc = "the partition to export, ignored if exportall (default 0)")
        int partition;

        @Option(desc = "Pair of sequence numbers specifying the range of rows to export (default 1,Long.MAX_VALUE), inclusive range")
        String range = "";

        @Option(desc = "only scan for gaps, default false (range is ignored)")
        boolean onlyscan = false;

        @Option(desc = "the count of exporting threads to use (default 20)")
        int threads = 20;

        @Option(desc = "do not print usage on error (default = false), used for bash encapsulation")
        boolean nousage = false;

       @Override
        public void validate() {
            if (StringUtils.isBlank(indir)) exitWithMessage("Need full path to export_overflow or files to parse");
            if (StringUtils.isBlank(catalog)) exitWithMessage("Need full path to catalog file");
            if (StringUtils.isBlank(outdir)) {
                if (!onlyscan) LOG.info("Exporting to same input directory ...");
                outdir = indir;
            }
            if (!exportall) {
                if (StringUtils.isBlank(stream_name)) exitWithMessage("Need stream_name for files to parse");
            }
            if (threads <= 0) exitWithMessage("threads must be > 0");
        }

        @Override
        public Object clone() throws CloneNotSupportedException {
            // shallow copy
            VoltExportConfig c = (VoltExportConfig)super.clone();
            return c;
        }

        public void exitWithMessage(String msg) {
            System.err.println(msg);
            if (!nousage) {
                printUsage();
            }
            System.exit(-1);
        }
    }

    private static VoltExportConfig s_cfg = new VoltExportConfig();

    // FIXME: may support different export targets in the future, only FILE is supported for now
    static enum Target {
        FILE(ExportToFileClient::new),
        JDBC(JDBCExportClient::new);

        private final Supplier<ExportClientBase> m_factory;

        Target(Supplier<ExportClientBase> factory) {
            m_factory = factory;
        }

        public ExportClientBase create() {
            return m_factory.get();
        }
    }
    static Target DEFAULT_TARGET = Target.FILE;

    public static void main(String[] args) throws IOException {
        s_cfg.parse(VoltExport.class.getName(), args);

        VoltExport ve = new VoltExport();
        ve.run();
    }

    void run() throws IOException {
        ArrayList<ExportClientBase> exportClients = new ArrayList<>();
        try {
            // Set up dummy ExportManager to enable E3 behavior
            VoltDB.resetSingletonsForTest();
            VoltDB.setExportManagerInstance(new DummyManager());

            // Set the root directory of the FILE export client
            ExportToFileClient.TEST_VOLTDB_ROOT = System.getProperty("user.dir");

            // Check directories
            File indir = new File(s_cfg.indir);
            if (!indir.canRead()) {
                s_cfg.exitWithMessage("Cannot read input directory " + indir.getAbsolutePath());
            }

            // Get database from catalog
            Database db = getDatabase();
            if (db == null) {
                s_cfg.exitWithMessage("No database in catalog " + s_cfg.catalog);
            }

            // Parse input directory to identify streams and partitions
            Set<Pair<String, Integer>> streamSet = new ExportFileVisitor(s_cfg.indir, db).visit();

            // Run exports
            if (!s_cfg.exportall) {
                if (!streamSet.contains(Pair.of(s_cfg.stream_name, s_cfg.partition))) {
                    LOG.errorFmt("No PBD files found for stream %s, partition %d in directory %s",
                            s_cfg.stream_name, s_cfg.partition, s_cfg.indir);
                    System.exit(-1);
                }

                // Run an ExportRunner synchronously
                ExportClientBase exportClient = createExportClient(DEFAULT_TARGET, s_cfg.stream_name, s_cfg.partition);
                exportClients.add(exportClient);
                ExportRunner runner = new ExportRunner(s_cfg, exportClient, db);
                runner.call();
            }
            else {
                if (streamSet.isEmpty()) {
                    LOG.errorFmt("No PBD files found for any stream in directory %s", s_cfg.indir);
                    System.exit(-1);
                }

                // Run ExportRunners in threadpool
                ExecutorService executor = Executors.newFixedThreadPool(s_cfg.threads);
                ArrayList<ExportRunner> tasks = new ArrayList<>();

                for (Pair<String, Integer> topicPartition : streamSet) {
                    VoltExportConfig cfg = (VoltExportConfig)s_cfg.clone();
                    cfg.exportall = false;
                    cfg.stream_name = topicPartition.getFirst();
                    cfg.partition = topicPartition.getSecond().intValue();
                    ExportClientBase exportClient = createExportClient(DEFAULT_TARGET, cfg.stream_name, cfg.partition);
                    exportClients.add(exportClient);
                    tasks.add(new ExportRunner(cfg, exportClient, db));
                }

                LOG.infoFmt("Starting %d export runners ...", tasks.size());
                List<Future<VoltExportResult>> results = executor.invokeAll(tasks);

                LOG.infoFmt("Waiting for %d export runner completions ...", results.size());
                executor.shutdown();
                int minutes = 0;
                while (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                    LOG.infoFmt("... still waiting for %d export completions after %d minutes", results.size(), ++minutes);
                }

                int exceptions = 0;
                for (Future<VoltExportResult> fut : results) {
                    try {
                        fut.get();
                    }
                    catch (Exception e) {
                        exceptions++;
                    }
                }
                if (exceptions > 0) {
                    LOG.errorFmt("%d export runners encountered exceptions", exceptions);
                }
            }
        }
        catch (Exception e) {
            LOG.error("Failed exporting");
            e.printStackTrace();
        }
        finally {
            for (ExportClientBase exportClient : exportClients) {
                try {
                exportClient.shutdown();
                }
                catch(Exception e) {
                    LOG.error("Failed shutting down export client");
                    e.printStackTrace();
                }
            }
        }
        if (s_cfg.exportall) {
            LOG.infoFmt("Finished exporting all streams in directory %s", s_cfg.indir);
        }
        else {
            LOG.infoFmt("Finished exporting stream %s, partition %d in directory %s",
                    s_cfg.stream_name, s_cfg.partition, s_cfg.indir);
        }
    }

    public static Database getDatabase() throws IOException {
        InMemoryJarfile imjf = new InMemoryJarfile(s_cfg.catalog);
        return CatalogUtil.getDatabaseFrom(imjf);
    }

    private Properties getProperties(Target target, String name, int partition) throws IOException {
        Properties properties = new Properties();
        if (StringUtils.isBlank(s_cfg.properties)) {
            LOG.infoFmt("No properties specifed for target %s", target);
        } else {
            final InputStream in;

            File propFile = new File(s_cfg.properties);
            if (propFile.exists()) {
                in = new FileInputStream(propFile);
            } else {
                in = new StringInputStream(s_cfg.properties);
            }
            try (InputStream i = in) {
                properties.load(i);
            }
        }

        // Do some property checks and adjustments
        if (target == Target.FILE) {
            // File export, set the nonce to stream_partition
            String nonce = name + "_" + partition;
            properties.put("nonce", nonce);
            properties.put("outdir", s_cfg.outdir);
        }
        return properties;
    }

    private ExportClientBase createExportClient(Target target, String name, int partition)
            throws ClassNotFoundException, Exception {
        ExportClientBase client = target.create();
        client.configure(getProperties(target, name, partition));
        client.setTargetName(s_cfg.stream_name);
        return client;
    }

    public static class DummyManager implements ExportManagerInterface {

        // Pretend we're running E3
        @Override
        public ExportMode getExportMode() {
            return ExportMode.ADVANCED;
        }

        @Override
        public void clearOverflowData() throws SetupException {
        }

        @Override
        public int getConnCount() {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public Generation getGeneration() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public ExportStats getExportStats() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public int getExportTablesCount() {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public List<ExportStatsRow> getStats(boolean interval) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public void startListeners(ClientInterface cif) {
            // TODO Auto-generated method stub

        }

        @Override
        public void shutdown() {
            // TODO Auto-generated method stub

        }

        @Override
        public void updateInitialExportStateToSeqNo(int partitionId, String streamName, StreamStartAction action,
                Map<Integer, ExportSnapshotTuple> sequenceNumberPerPartition) {
            // TODO Auto-generated method stub

        }

        @Override
        public void updateDanglingExportStates(StreamStartAction action,
                Map<String, Map<Integer, ExportSnapshotTuple>> exportSequenceNumbers) {
            // TODO Auto-generated method stub

        }

        @Override
        public void sync() {
            // TODO Auto-generated method stub

        }

        @Override
        public void invokeMigrateRowsDelete(int partition, String tableName, long deletableTxnId,
                ProcedureCallback cb) {
            // TODO Auto-generated method stub

        }

        @Override
        public void waitOnClosingSources() {
            // TODO Auto-generated method stub

        }

        @Override
        public void onDrainedSource(String tableName, int partition) {
            // TODO Auto-generated method stub

        }

        @Override
        public void onClosingSource(String tableName, int partition) {
            // TODO Auto-generated method stub

        }

        @Override
        public void onClosedSource(String tableName, int partition) {
            // TODO Auto-generated method stub

        }

        @Override
        public void releaseResources(List<Integer> removedPartitions) {
            // TODO Auto-generated method stub

        }

        @Override
        public void initialize(CatalogContext catalogContext, Map<Integer, Integer> localPartitionsToSites,
                boolean isRejoin) {
            // TODO Auto-generated method stub

        }

        @Override
        public void startPolling(CatalogContext catalogContext) {
            // TODO Auto-generated method stub

        }

        @Override
        public void updateCatalog(CatalogContext catalogContext, boolean requireCatalogDiffCmdsApplyToEE,
                boolean requiresNewExportGeneration, Map<Integer, Integer> localPartitionsToSites) {
            // TODO Auto-generated method stub

        }

        @Override
        public void processExportControl(String exportSource, List<String> exportTargets,
                StreamControlOperation operation, VoltTable results) {
            // TODO Auto-generated method stub

        }

        @Override
        public void pushBuffer(int partitionId, String tableName, long startSequenceNumber,
                long committedSequenceNumber, long tupleCount, long uniqueId, long committedSpHandle,
                BBContainer buffer) {
            // TODO Auto-generated method stub

        }

    }
}
