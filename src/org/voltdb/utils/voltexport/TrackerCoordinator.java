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

import java.util.Map;
import java.util.TreeMap;

import org.voltcore.utils.Pair;
import org.voltdb.e3.E3ExportCoordinator;
import org.voltdb.export.ExportSequenceNumberTracker;

import com.google_voltpatches.common.collect.ImmutableMap;

/**
 * A class that fills gaps in a map of hostId -> {@link ExportSequenceNumberTracker}
 * <p>
 * Extracted and refactored from 9.3.x {@link E3ExportCoordinator}, because evaluation logic changed
 * in subsequent versions and we want a unique evaluation logic to debug in this tool.
 * <p>
 * Note: not optimized in all cases: it may select a leader with gaps and ignore a replica
 * without gaps: the replica will fill the gaps of the leader but it would have been unnecessary.
 * We might want to improve this logic and add more testing of edge cases: add a main() function
 * invoking tests.
 */
public class TrackerCoordinator {
    private final boolean m_debug;

    // The map of hostId -> original trackers
    private final ImmutableMap<Integer, ExportSequenceNumberTracker> m_trackers;

    private static final int NO_HOST_ID =  -1;

    public TrackerCoordinator(boolean debug, Map<Integer, ExportSequenceNumberTracker> trackers) {
        m_debug = debug;

        ImmutableMap.Builder<Integer, ExportSequenceNumberTracker> b =
                new ImmutableMap.Builder<Integer, ExportSequenceNumberTracker>().putAll(trackers);
        m_trackers = b.build();
    }

    /**
     * Return a map of hostId -> master trackers
     * <p>
     * A 'master' tracker is an {@ExportSequenceNumberTracker} that contains ranges of
     * export rows (identified by their sequence number in the tracker), that don't overlap
     * with another master tracker in the map.
     * <p>
     * The map of master trackers that is returned fills the gaps in the original
     * map of trackers, and avoids duplicates in the corresponding ranges of
     * export rows.
     *
     * @return the map of hostId -> master trackers
     */
    public Map<Integer, ExportSequenceNumberTracker> getMasterTrackers() {
        Map<Integer, ExportSequenceNumberTracker> masters = new TreeMap<>();
        int leaderId = NO_HOST_ID;

        for (Integer hostId : m_trackers.keySet()) {
            if (!m_trackers.containsKey(hostId)) {
                continue;
            }

            // Make the first host the leader, keep its original tracker as master for this host
            if (leaderId == NO_HOST_ID) {
                leaderId = hostId;
                masters.put(hostId, m_trackers.get(hostId));
                continue;
            }

            // Get the master tracker for this host
            Pair<Long, ExportSequenceNumberTracker> trk = new Pair<>(0L, new ExportSequenceNumberTracker());
            while(trk.getFirst() != ExportSequenceNumberTracker.INFINITE_SEQNO) {
                trk = buildMasterTracker(leaderId, hostId, trk.getFirst(), trk.getSecond());
            }
            masters.put(hostId,  trk.getSecond());
        }
        return masters;
    }

    /**
     * Build a 'master' tracker for a host, that fills gaps of a leader host
     * <p>
     * The tracker is built recursively until we find no more segments to add to the master tracker.
     *
     * @param leaderId      the hostId of the leader
     * @param myId          the hostId of the host we're building the tracker for
     * @param safePoint     the current safe point being evaluated
     * @param masterTracker the tracker being built, recursively
     * @return  <safePoint, masterTracker>,
     *          with safePoint == next safePpoint to evaluate,
     *          or {@link ExportSequenceNumberTracker.INFINITE_SEQNO} indicating we are done.
     */
    private Pair<Long, ExportSequenceNumberTracker> buildMasterTracker(int leaderId, int myId, long safePoint,
            ExportSequenceNumberTracker masterTracker) {

        assert leaderId != myId : "Don't build master tracker for leader";
        long exportSeqNo = safePoint + 1;
        ExportSequenceNumberTracker leaderTracker = m_trackers.get(leaderId);
        assert leaderTracker != null : "No leader tracker";

        // Get the first gap on the leader covering or following this sequence number
        Pair<Long, Long> gap = leaderTracker.getFirstGap(exportSeqNo);
        assert (gap == null || exportSeqNo <= gap.getSecond());
        if (gap == null || exportSeqNo < (gap.getFirst() - 1)) {

            if (gap == null) {
                // We have finished building this tracker
                safePoint = ExportSequenceNumberTracker.INFINITE_SEQNO;
            } else {
                safePoint = gap.getFirst() - 1;
            }

            if (m_debug) LOG.debugFmt("Leader %d is master until safe point %d", leaderId, safePoint);
            return new Pair<Long, ExportSequenceNumberTracker>(safePoint, masterTracker);
        }

        // Find the lowest hostId that can fill the gap: run this exact sequence for every 'myId',
        // so that gap filling is consistent across all hostIds
        assert (gap != null);
        if (m_debug) LOG.debugFmt("Leader %d at seqNo %d, hits gap [%d, %d], look for candidate replicas",
                leaderId, exportSeqNo, gap.getFirst() , gap.getSecond());

        Integer replicaId = NO_HOST_ID;
        long leaderNextSafePoint = gap.getSecond();
        long  replicaSafePoint = 0L;

        for (Integer hostId : m_trackers.keySet()) {

            if (leaderId == hostId.intValue()) {
                continue;
            }
            Pair<Long, Long> rgap = m_trackers.get(hostId).getFirstGap(exportSeqNo);
            if (m_debug) LOG.debugFmt("Evaluating Replica %d,  gap %s, for seqNo %d" + exportSeqNo, hostId, rgap, exportSeqNo);
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

            if (m_debug) LOG.debugFmt("Replica %d %s fills gap [%d,%d], until safe point %d",
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

        if (m_debug) LOG.debugFmt("Leader %d is master for %d, blocked at safe point %d; resume evaluation at %d",
                leaderId, exportSeqNo, gap.getFirst(), safePoint);

        return buildMasterTracker(leaderId, myId, safePoint, masterTracker);
    }
}
