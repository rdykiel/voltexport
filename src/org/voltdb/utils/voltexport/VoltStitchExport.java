/* This file is part of VoltDB.
 * Copyright (C) 2022 VoltDB Inc.
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

import static org.voltdb.utils.voltexport.VoltExport.LOG;

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
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.voltdb.CLIConfig;
import org.voltdb.export.ExportManagerInterface;
import org.voltdb.export.ExportSequenceNumberTracker;
import org.voltdb.exportclient.ExportClientBase;
import org.voltdb.exportclient.ExportToFileClient;
import org.voltdb.utils.StringInputStream;
import org.voltdb.utils.voltexport.VoltExport.DummyManager;
import org.voltdb.utils.voltexport.VoltExport.VoltExportConfig;

/**
 * A class that 'stitches' an export stream from multiple nodes and exports the result in csv.
 * <p>
 * Note: restricted to 1 stream/partition, may be extended to all stream/partitions later.
 */
public class VoltStitchExport {

    /**
     * Configuration options
     */
    public static class VoltStitchExportConfig extends CLIConfig implements Cloneable {

        @Option(desc = "input directory list - required")
        String indirs = "";

        @Option(desc = "output directory for file export - required")
        String outdir = "";

        @Option(desc = "Properties file or a string which can be parsed as a properties file, for export target configuration")
        String properties = "";

        @Option(desc = "stream name to export - required")
        String stream_name = "";

        @Option(desc = "the partition to export (default 0)")
        int partition;

        @Option(desc = "do not print usage on error (default = false), used for bash encapsulation")
        boolean nousage = false;

        @Option(desc = "print debug usage on error (default = false), used for bash encapsulation")
        boolean debug = false;

        @Option(desc = "the count of exporting threads to use (default 20)")
        int threads = 20;

       @Override
        public void validate() {
            if (StringUtils.isBlank(indirs)) exitWithMessage("Need list of export overflow directories");
            if (StringUtils.isBlank(outdir)) exitWithMessage("Need output directory");
            if (StringUtils.isBlank(stream_name)) exitWithMessage("Need stream_name for files to parse");
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

    private static VoltStitchExportConfig s_cfg = new VoltStitchExportConfig();

    public static void main(String[] args) throws IOException {
        s_cfg.parse(VoltStitchExport.class.getName(), args);

        VoltStitchExport vse = new VoltStitchExport();
        vse.run();
    }

    void run() throws IOException {
        try {
            // Get array of input directories, index in array becomes hostId
            ArrayList<String> indirs = getInputDirs(s_cfg.indirs);
            for (int hostId = 0; hostId < indirs.size(); hostId++) {
                LOG.infoFmt("Host %d: %s", hostId, indirs.get(hostId));
            }

            // Set up dummy ExportManager to enable E3 behavior
            ExportManagerInterface.setInstanceForTest(new DummyManager());

            // Set the root directory of the FILE export client
            ExportToFileClient.TEST_VOLTDB_ROOT = System.getProperty("user.dir");

            // Get original trackers for all hosts. Note, some host may have no trackers
            // FIXME: could optimize with ConcurrentHashMap and parallel scanning
            Map<Integer, ExportSequenceNumberTracker> trackers = getTrackers(indirs);
            if (trackers.isEmpty()) {
                LOG.errorFmt("No PBD files found in directories %s", indirs);
                return;
            }
            for (int hostId = 0; hostId < indirs.size(); hostId++) {
                LOG.infoFmt("Host %d: %s", hostId, trackers.get(hostId));
            }

            // Compute master trackers per host - since we have non-empty trackers as input,
            // the resulting masters shouldn't be empty either
            TrackerCoordinator tc = new TrackerCoordinator(s_cfg.debug, trackers);
            Map<Integer, ExportSequenceNumberTracker> masters = tc.getMasterTrackers();

            assert !masters.isEmpty() : "No master trackers";
            masters.forEach((k, v) -> LOG.infoFmt("Host %d mastership: %s", k, v));

            // Run SegmentsRunner instances in threadpool: export all hosts in parallel
            ExecutorService executor = Executors.newFixedThreadPool(s_cfg.threads);
            Properties props = loadProperties();
            ArrayList<SegmentsRunner> tasks = new ArrayList<>();
            long totalRows = 0;

            for (Map.Entry<Integer, ExportSequenceNumberTracker> e : masters.entrySet()) {
                int hostId = e.getKey().intValue();
                ExportSequenceNumberTracker trk = e.getValue();

                assert !trk.isEmpty() : "Empty master tracker for " + hostId;
                totalRows += trk.sizeInSequence();
                tasks.add(new SegmentsRunner(hostId, indirs.get(hostId), s_cfg.outdir,
                        s_cfg.stream_name, s_cfg.partition, trk, props));
            }

            LOG.infoFmt("Starting %d segments runners for a total of %d rows to export ...", tasks.size(), totalRows);
            List<Future<Integer>> results = executor.invokeAll(tasks);

            LOG.infoFmt("Waiting for %d segments runner completions ...", results.size());
            executor.shutdown();
            int minutes = 0;
            while (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                LOG.infoFmt("... still waiting for %d segments completions after %d minutes", results.size(), ++minutes);
            }

            int errors = 0;
            for (Future<Integer> fut : results) {
                try {
                    errors += fut.get().intValue();
                }
                catch (Exception e) {
                    errors++;
                    e.printStackTrace();
                }
            }
            if (errors > 0) {
                LOG.errorFmt("Segment runners encountered %d errors", errors);
            }
            else {
                LOG.infoFmt("Success: stitching %d rows of %s:%d COMPLETE", totalRows, s_cfg.stream_name, s_cfg.partition);
            }
        }
        catch (Exception e) {
            LOG.errorFmt("Failed stitching %s, partition %d", s_cfg.stream_name, s_cfg.partition);
            e.printStackTrace();
        }
    }

    private ArrayList<String> getInputDirs(String dirList) {
        String[] dirs = dirList.split(",");
        Set<String> filtered = new HashSet<>();
        for(String dir : dirs) {
            dir = dir.trim();
            if (StringUtils.isEmpty(dir)) {
                continue;
            }
            if (!filtered.add(dir)) {
                LOG.warnFmt("Directory %s specified more than once in inputs", dir);
            }
        }
        return new ArrayList<String>(filtered);
    }

    Map<Integer, ExportSequenceNumberTracker> getTrackers(ArrayList<String> indirs) throws ClassNotFoundException, Exception {
        Map<Integer, ExportSequenceNumberTracker> trackers = new HashMap<>();
        ArrayList<ExportClientBase> exportClients = new ArrayList<>();

        try {
            // Build array of export clients, using hostId for the sequence numbers:
            // we're just scanning here, not exporting, so no output file gets created
            for (int hostId = 0; hostId < indirs.size(); hostId++) {
                exportClients.add(createExportClient(hostId, hostId));
            }

            // Get the trackers of each host in proper sequence
            for (int hostId = 0; hostId < indirs.size(); hostId++) {
                VoltExportConfig cfg = new VoltExportConfig();
                cfg.indir = indirs.get(hostId);
                cfg.stream_name = s_cfg.stream_name;
                cfg.partition = s_cfg.partition;
                cfg.onlyscan = true;

                ExportRunner runner = new ExportRunner(cfg, exportClients.get(hostId));
                VoltExportResult res = runner.call();
                if (res.success && !res.tracker.isEmpty()) {
                    trackers.put(hostId, res.tracker);
                }
            }
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

        return trackers;
    }

    private ExportClientBase createExportClient(long startSeq, long endSeq)
            throws ClassNotFoundException, Exception {
        ExportClientBase client = new ExportToFileClient();
        client.configure(getProperties(startSeq, endSeq));
        client.setTargetName(s_cfg.stream_name);
        return client;
    }

    private Properties getProperties(long startSeq, long endSeq) throws IOException {
        Properties properties = loadProperties();
        // Set the nonce to stream_partition_startSeq_endSeq
        String nonce = SegmentsRunner.getNonce(s_cfg.stream_name, s_cfg.partition, startSeq, endSeq);
        properties.put("nonce", nonce);
        properties.put("outdir", s_cfg.outdir);

        return properties;
    }

    private Properties loadProperties() throws IOException {
        Properties properties = new Properties();
        if (StringUtils.isBlank(s_cfg.properties)) {
            LOG.info("No properties specified ...");
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
        return properties;
    }
}
