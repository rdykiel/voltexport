/* This file is part of VoltDB.
 * Copyright (C) 2008-2020 VoltDB Inc.
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
import org.voltdb.VoltTable;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.export.ExportDataSource.StreamStartAction;
import org.voltdb.export.ExportManagerInterface;
import org.voltdb.export.ExportStats;
import org.voltdb.export.Generation;
import org.voltdb.exportclient.ExportClientBase;
import org.voltdb.exportclient.ExportClientBase.DecodingPolicy;
import org.voltdb.exportclient.ExportToFileClient;
import org.voltdb.exportclient.JDBCExportClient;
import org.voltdb.sysprocs.ExportControl.OperationMode;
import org.voltdb.utils.StringInputStream;

import com.google_voltpatches.common.base.Splitter;

public class VoltExport {
    public static final VoltLogger LOG = new VoltLogger("VOLTEXPORT");

    /**
     * Configuration options.
     */
    public static class VoltExportConfig extends CLIConfig {

        @Option(desc = "export_overflow directory (or location of saved export files)")
        String export_overflow = "";

        @Option(desc = "Properties file or a string which can be parsed as a properties file, for export target configuration")
        String properties = "outdir=/tmp/voltexport_out";

        @Option(desc = "stream name to export")
        String stream_name = "";

        @Option(desc = "partitions to export (comma-separated, default all partitions)")
        String partitions = "";

        @Option(desc = "list of skip entries allowing skipping rows to export (comma-separated, default no skipping): each list element as \'X:Y\' (X=partition, Y=count)")
        String skip = "";

        @Override
        public void printUsage() {
            super.printUsage();
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
        ExportClientBase exportClient = null;
        Set<Integer> partitions = new HashSet<>();
        try {
            // Set up dummy ExportManager to enable E3 behavior
            ExportManagerInterface.setInstanceForTest(new DummyManager());

            // Set the root directory of the FILE export client
            ExportToFileClient.TEST_VOLTDB_ROOT = System.getProperty("user.dir");

            // Check directories
            if (StringUtils.isBlank(s_cfg.export_overflow)) {
                s_cfg.exitWithMessageAndUsage("Missing --export_overflow parameter");
            }
            File indir = new File(s_cfg.export_overflow);
            if (!indir.canRead()) {
                s_cfg.exitWithMessageAndUsage("Cannot read input directory " + indir.getAbsolutePath());
            }

            // Parse requested partitions
            List<Integer> requestedPartitions = getRequestedPartitions();

            // Parse input directory to identify streams and partitions
            File files[] = indir.listFiles();
            if (files == null || files.length == 0) {
                s_cfg.exitWithMessageAndUsage("No files in input directory " + indir.getAbsolutePath());
            }

            for (File data: files) {
                if (data.getName().endsWith(".pbd")) {
                    // Note: PbdSegmentName#parseFile bugs here
                    Pair<String, Integer> topicPartition = getTopicPartition(data.getName());
                    if (s_cfg.stream_name.equalsIgnoreCase(topicPartition.getFirst())) {
                        if (partitions.add(topicPartition.getSecond())) {
                            LOG.info("Detected stream " + topicPartition.getFirst() + " partition " + topicPartition.getSecond());
                        }
                    }
                    else if (LOG.isDebugEnabled()) {
                        LOG.debug("Ignoring " + data.getName()) ;
                    }
                }
            }

            // Verify the requested partitions
            if (requestedPartitions != null) {
                for (Integer partition : requestedPartitions) {
                    if (!partitions.contains(partition)) {
                        s_cfg.exitWithMessageAndUsage("Unknown partition " + partition);
                    }
                }
                partitions.clear();
                partitions.addAll(requestedPartitions);
            }

            // Parse an optional skipList
            Map<Integer, Long> skipRows = getSkipRows(partitions);

            // Create client
            exportClient = createExportClient(DEFAULT_TARGET);

            // Run ExportRunners per the exportClient's decoding policy
            int threads = exportClient.getDecodingPolicy() == DecodingPolicy.BY_PARTITION_TABLE ? partitions.size() : 1;
            ExecutorService es = Executors.newFixedThreadPool(threads);
            for (Integer partition : partitions) {
                es.execute(new ExportRunner(s_cfg, partition, exportClient, skipRows.getOrDefault(partition, 0L)));
            }

            es.shutdown();
            es.awaitTermination(1, TimeUnit.DAYS);
        }
        catch (Exception e) {
            LOG.error("Failed exporting", e);
        }
        finally {
            if (exportClient != null) {
                try {
                exportClient.shutdown();
                }
                catch(Exception e) {
                    LOG.error("Failed shutting down export client", e);
                }
            }
        }
        LOG.info("Finished exporting " + partitions.size() + " partitions of stream " + s_cfg.stream_name);
    }

    List<Integer> getRequestedPartitions() {
        if (StringUtils.isBlank(s_cfg.partitions)) {
            return null;
        }
        List<String> partitionList = Splitter.on(',').trimResults().splitToList(s_cfg.partitions);
        ArrayList<Integer> partitions = new ArrayList<>(partitionList.size());

        for(String partitionStr : partitionList) {
            try {
                partitions.add(Integer.parseInt(partitionStr));
            }
            catch (NumberFormatException e) {
                s_cfg.exitWithMessageAndUsage(partitionStr + " is not a valid partition");
            }
        }

        return partitions;
    }

    /**
     * Build a map of partitions -> rows to skip. Ignores unknown or undesired partitions
     *
     * @param partitions set of requested partitions
     * @return a Map<partition, skip> of the skip rows, never {@code null}
     */
    Map<Integer, Long> getSkipRows(Set<Integer> partitions) {
        Map<Integer, Long> skipRows = new HashMap<>();
        if (StringUtils.isBlank(s_cfg.skip)) {
            return skipRows;
        }

        List<String> partitionList = Splitter.on(',').trimResults().splitToList(s_cfg.skip);
        for(String partitionStr : partitionList) {
            List<String> splitArgs = Splitter.on(':').trimResults().splitToList(partitionStr);
            try {
                Integer partition = Integer.parseInt(splitArgs.get(0));
                Long skip = Long.parseLong(splitArgs.get(1));

                if (!partitions.contains(partition)) {
                    LOG.warn("Ignoring unknown or unwanted skip partition " + partition);
                }
                else {
                    skipRows.put(partition, skip);
                }
            }
            catch (Exception e) {
                s_cfg.exitWithMessageAndUsage(partitionStr + " is not a valid split list (\"X:Y\", X = partition, Y = rows to skip)");
            }
        }
        return skipRows;
    }

    // Totally inefficient PBD file name parsing
    // Note: I don't know why PbdSegmentName#parseFile bugs here when I run this from the jar
    // Naming convention for export pdb file: [table name]_[partition]_[segmentId]_[prevId].pdb,
    private Pair<String, Integer> getTopicPartition(String fileName) {

        String meatloaf = fileName;
        int lastIndex = meatloaf.lastIndexOf('_');
        if (lastIndex == -1) {
            throw new IllegalArgumentException("Bad file name: " + fileName);
        }
        meatloaf = meatloaf.substring(0, lastIndex);
        lastIndex = meatloaf.lastIndexOf('_');
        if (lastIndex == -1) {
            throw new IllegalArgumentException("Bad file name: " + fileName);
        }
        meatloaf = meatloaf.substring(0, lastIndex);
        lastIndex = meatloaf.lastIndexOf('_');
        if (lastIndex == -1) {
            throw new IllegalArgumentException("Bad file name: " + fileName);
        }
        String streamName = meatloaf.substring(0, lastIndex);
        String partitionStr = meatloaf.substring(lastIndex + 1);
        Integer partition = Integer.parseInt(partitionStr);
        return Pair.of(streamName, partition);
    }

    private Properties getProperties(Target target) throws IOException {
        Properties properties = new Properties();
        if (s_cfg.properties == null) {
            LOG.info("No properties specifed for target " + target);
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
            String nonce = properties.getProperty("nonce");
            if (nonce == null) {
                nonce = s_cfg.stream_name.toUpperCase();
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Setting nonce to " + nonce + " for " + target + " export");
                }
                properties.put("nonce", nonce);
            }
            LOG.info("Exporting " + target + " to directory "
                    + ExportToFileClient.TEST_VOLTDB_ROOT + "/" + properties.getProperty("outdir"));
        }
        return properties;
    }

    private ExportClientBase createExportClient(Target target)
            throws ClassNotFoundException, Exception {
        ExportClientBase client = target.create();
        client.configure(getProperties(target));
        client.setTargetName(s_cfg.stream_name);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Created export client " + client.getClass().getName());
        }
        return client;
    }

    private static class DummyManager implements ExportManagerInterface {

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
        public void initialize(CatalogContext catalogContext, List<Pair<Integer, Integer>> localPartitionsToSites,
                boolean isRejoin) {
            // TODO Auto-generated method stub

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
        public void startPolling(CatalogContext catalogContext, StreamStartAction action) {
            // TODO Auto-generated method stub

        }

        @Override
        public void updateCatalog(CatalogContext catalogContext, boolean requireCatalogDiffCmdsApplyToEE,
                boolean requiresNewExportGeneration, List<Pair<Integer, Integer>> localPartitionsToSites) {
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
        public void processStreamControl(String exportSource, List<String> exportTargets, OperationMode valueOf,
                VoltTable results) {
            // TODO Auto-generated method stub

        }

        @Override
        public void pushBuffer(int partitionId, String tableName, long startSequenceNumber,
                long committedSequenceNumber, long tupleCount, long uniqueId, BBContainer buffer) {
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

    }
}
