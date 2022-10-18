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
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;
import org.voltcore.utils.Pair;
import org.voltdb.CLIConfig;
import org.voltdb.e3.E3ExportCoordinator;
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

       @Override
        public void validate() {
            if (StringUtils.isBlank(indirs)) exitWithMessage("Need list of export overflow directories");
            if (StringUtils.isBlank(outdir)) exitWithMessage("Need output directory");
            if (StringUtils.isBlank(stream_name)) exitWithMessage("Need stream_name for files to parse");
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
    private static final int NO_HOST_ID =  -1;

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

            // Compute master trackers per host
            Map<Integer, ExportSequenceNumberTracker> masters = new TreeMap<>();
            int leaderId = NO_HOST_ID;

            // HACK - REMOVEME
            leaderId = 1;
            masters.put(leaderId, trackers.get(leaderId));

            for (Integer hostId : trackers.keySet()) {
                if (!trackers.containsKey(hostId)) {
                    continue;
                }
                else if (hostId == leaderId) {
                    continue;   // HACKJ - REMOVEME
                }

                // Make the first host the leader, keep its original tracker as master for this host
                if (leaderId == NO_HOST_ID) {
                    leaderId = hostId;
                    masters.put(hostId, trackers.get(hostId));
                    continue;
                }

                // Get the master tracker for this host
                LOG.infoFmt("Get master tracker for host %d ...", hostId);
                Pair<Long, ExportSequenceNumberTracker> trk = new Pair<>(0L, new ExportSequenceNumberTracker());
                while(trk.getFirst() != ExportSequenceNumberTracker.INFINITE_SEQNO) {
                    trk = buildMasterTracker(leaderId, hostId, trk.getFirst(), trk.getSecond(), trackers);
                }
                masters.put(hostId,  trk.getSecond());
            }

            masters.forEach((k, v) -> LOG.infoFmt("Host %d mastership: %s", k, v));
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
            // Build array of export clients, using hostId as the sequence number
            // (we're just scanning here, not exporting)
            for (int hostId = 0; hostId < indirs.size(); hostId++) {
                exportClients.add(createExportClient(hostId));
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

    private ExportClientBase createExportClient(int seqNum)
            throws ClassNotFoundException, Exception {
        ExportClientBase client = new ExportToFileClient();
        client.configure(getProperties(seqNum, seqNum));
        client.setTargetName(s_cfg.stream_name);
        return client;
    }

    private Properties getProperties(long startSeq, long endSeq) throws IOException {
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

        // Set the nonce to stream_partition_startSeq_endSeq_
        String nonce = String.format("%s_%d_%d_%d_", s_cfg.stream_name, s_cfg.partition, startSeq, endSeq);
        properties.put("nonce", nonce);
        properties.put("outdir", s_cfg.outdir);

        return properties;
    }

    /**
     * Build a 'master' tracker for a host, that fills gaps of a leader host
     * <p>
     * Extracted from 9.3.x {@link E3ExportCoordinator}, because evaluation logic changed
     * in subsequent versions and we want a unique evaluation logic to debug in this tool.
     * <p>
     * The tracker is built recursively until we find no more segments to add to the master tracker.
     *
     * @param leaderId      the hostId of the leader
     * @param myId          the hostId of the host we're building the tracker for
     * @param safePoint     the current safe point being evaluated
     * @param masterTracker the tracker being built, recursively
     * @param trackers      the map of trackers per host
     * @return  <safepoint, masterTracker>
     */
    private Pair<Long, ExportSequenceNumberTracker> buildMasterTracker(int leaderId, int myId, long safePoint,
            ExportSequenceNumberTracker masterTracker, Map<Integer, ExportSequenceNumberTracker> trackers) {

        assert leaderId != myId : "Don't build master tracker for leader";
        long exportSeqNo = safePoint + 1;
        ExportSequenceNumberTracker leaderTracker = trackers.get(leaderId);
        assert leaderTracker != null : "No leader tracker";

        // Get the first gap covering or following this sequence number on the leader
        Pair<Long, Long> gap = leaderTracker.getFirstGap(exportSeqNo);
        assert (gap == null || exportSeqNo <= gap.getSecond());
        if (gap == null || exportSeqNo < (gap.getFirst() - 1)) {

            if (gap == null) {
                // We have finished building this tracker
                safePoint = ExportSequenceNumberTracker.INFINITE_SEQNO;
            } else {
                safePoint = gap.getFirst() - 1;
            }

            if (s_cfg.debug) LOG.debugFmt("Leader %d is master until safe point %d", leaderId, safePoint);
            return new Pair<Long, ExportSequenceNumberTracker>(safePoint, masterTracker);
        }

        // Find the lowest hostId that can fill the gap
        assert (gap != null);
        if (s_cfg.debug) LOG.debugFmt("Leader %d at seqNo %d, hits gap [%d, %d], look for candidate replicas",
                leaderId, exportSeqNo, gap.getFirst() , gap.getSecond());

        Integer replicaId = NO_HOST_ID;
        long leaderNextSafePoint = gap.getSecond();
        long  replicaSafePoint = 0L;

        for (Integer hostId : trackers.keySet()) {

            if (leaderId == hostId.intValue()) {
                continue;
            }
            Pair<Long, Long> rgap = trackers.get(hostId).getFirstGap(exportSeqNo);
            if (s_cfg.debug) LOG.debugFmt("Evaluating Replica %d,  gap %s, for seqNo %d" + exportSeqNo, hostId, rgap, exportSeqNo);
            if (rgap != null) {
                assert (exportSeqNo <= rgap.getSecond());
            }
            if (rgap == null || exportSeqNo <= (rgap.getFirst() - 1)) {
                replicaId = hostId;
                if (rgap == null) {
                    // We have finished building this tracker
                    replicaSafePoint = ExportSequenceNumberTracker.INFINITE_SEQNO;
                } else {
                    // The next safe point of the replica is the last before the
                    // replica gap
                    replicaSafePoint = rgap.getFirst() -1;
                }
                break;
            }
            else {
                // the sequence number must be in the replica's gap
                assert (rgap.getFirst() <=  exportSeqNo && exportSeqNo <= rgap.getSecond());
                // memorize the closest replica's safe point
                long rgapSP = rgap.getSecond();
                replicaSafePoint = replicaSafePoint == 0 ? rgapSP : Math.min(replicaSafePoint, rgapSP);
            }
        }

        // Found replica
        if (!replicaId.equals(NO_HOST_ID)) {
            boolean isMasterMe = myId == replicaId.intValue();
            String localHost = isMasterMe ? " (localHost) " : "";
            safePoint = Math.min(leaderNextSafePoint, replicaSafePoint);

            if (s_cfg.debug) LOG.debugFmt("Replica %d %s fills gap [%d,%d], until safe point %d",
                    replicaId, localHost, gap.getFirst(), gap.getSecond(), safePoint);

            if (isMasterMe) {
                masterTracker.addRange(exportSeqNo, safePoint);
            }
            return new Pair<Long, ExportSequenceNumberTracker>(safePoint, masterTracker);
        }

        // If no replicas were found, the leader is Export Master and the gap will not be filled.
        // Continue building tracker past the gap, if we're not at the infinite seqNo.
        safePoint = replicaSafePoint != 0 ? Math.min(leaderNextSafePoint, replicaSafePoint) : leaderNextSafePoint;
        if (safePoint == ExportSequenceNumberTracker.INFINITE_SEQNO) {
            // Done
            return new Pair<Long, ExportSequenceNumberTracker>(safePoint, masterTracker);
        }

        if (s_cfg.debug) LOG.debugFmt("Leader %d is master for %d, blocked at safe point %d; resume evaluation at %d",
                leaderId, exportSeqNo, gap.getFirst(), safePoint);

        return buildMasterTracker(leaderId, myId, safePoint, masterTracker, trackers);
    }

}
