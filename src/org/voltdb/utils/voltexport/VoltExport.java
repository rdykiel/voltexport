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
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

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
import org.voltdb.sysprocs.ExportControl.OperationMode;

import com.google.common.collect.ImmutableMap;

public class VoltExport {
    public static final VoltLogger LOG = new VoltLogger("VOLTEXPORT");

    /**
     * Configuration options.
     */
    public static class VoltExportConfig extends CLIConfig {

        @Option(desc = "export_overflow directory (or location of saved export files")
        String export_overflow = "/tmp/export_overflow";

        @Option(desc = "output directory, location of exported CSV files")
        String out_dir = "/tmp/volt_export";

        @Option(desc = "stream name to export")
        String stream_name = "";
    }
    private static VoltExportConfig s_cfg = new VoltExportConfig();

    // FIXME: may support different export targets in the future (see CatalogUtil.java)
    // Only FILE is supported for now
    static enum Target {
        FILE,
        JDBC
    }
    static Target DEFAULT_TARGET = Target.FILE;

    ImmutableMap<Target, String> m_clients = ImmutableMap.of(
            Target.FILE, "org.voltdb.exportclient.ExportToFileClient",
            Target.JDBC, "org.voltdb.exportclient.JDBCExportClient");

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

            // Check directories
            File indir = new File(s_cfg.export_overflow);
            if (!indir.canRead()) {
                throw new IOException("Cannot read input directory " + indir.getAbsolutePath());
            }

            File outdir = new File(s_cfg.out_dir);
            if (!outdir.canWrite()) {
                throw new IOException("Cannot write output directory " + outdir.getAbsolutePath());
            }
            // Overwrite out_dir config with absolute path
            if (!s_cfg.out_dir.equals(outdir.getAbsolutePath())) {
                s_cfg.out_dir = outdir.getAbsolutePath();
            }

            // Parse input directory to identify streams and partitions
            File files[] = indir.listFiles();
            if (files == null || files.length == 0) {
                throw new IOException("No files in input directory " + indir.getAbsolutePath());
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

            Properties props = getProperties(DEFAULT_TARGET);
            exportClient = createExportClient(m_clients.get(DEFAULT_TARGET), props);

            // Run one ExportRunner per partition
            ArrayList<Thread> threads = new ArrayList<>(partitions.size());
            for (Integer partition : partitions) {
                threads.add(new Thread(new ExportRunner(s_cfg, partition, exportClient)));
            }

            threads.forEach(t -> t.start());
            threads.forEach(t -> {try {t.join(); } catch(Exception e) {}});
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

    private Properties getProperties(Target target) {
        Properties props = new Properties();
        switch(target) {
            case FILE:
                setFileProperties(props);
                break;
            default:
                break;
        }
        return props;
    }

    private void setFileProperties(Properties props) {
        props.put("nonce", s_cfg.stream_name.toUpperCase());
        props.put("outdir", s_cfg.out_dir);
    }

    private ExportClientBase createExportClient(String exportClientClassName, Properties properties)
            throws ClassNotFoundException, Exception {
        final Class<?> clientClass = Class.forName(exportClientClassName);
        ExportClientBase client = (ExportClientBase) clientClass.newInstance();
        client.configure(properties);
        client.setTargetName(s_cfg.stream_name);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Created export client " + exportClientClassName);
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
