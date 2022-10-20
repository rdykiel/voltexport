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

import static org.voltdb.utils.voltexport.VoltExport.LOG;
import static org.voltdb.utils.voltexport.VoltExport.VOLTLOG;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltcore.utils.Pair;
import org.voltdb.export.AdvertisedDataSource;
import org.voltdb.export.ExportSequenceNumberTracker;
import org.voltdb.export.StreamBlock;
import org.voltdb.exportclient.ExportClientBase;
import org.voltdb.exportclient.ExportDecoderBase;
import org.voltdb.exportclient.ExportDecoderBase.RestartBlockException;
import org.voltdb.exportclient.ExportRow;
import org.voltdb.exportclient.ExportRowSchema;
import org.voltdb.exportclient.ExportRowSchemaSerializer;
import org.voltdb.utils.BinaryDeque;
import org.voltdb.utils.BinaryDeque.BinaryDequeScanner;
import org.voltdb.utils.BinaryDequeReader;
import org.voltdb.utils.PersistentBinaryDeque;
import org.voltdb.utils.VoltFile;
import org.voltdb.utils.voltexport.VoltExport.VoltExportConfig;

public class ExportRunner implements Callable<VoltExportResult> {
    // Create a singleton scheduled thread pool for block processing timeouts
    private static final ScheduledThreadPoolExecutor s_timeoutExecutor =
            CoreUtils.getScheduledThreadPoolExecutor("Block Processing Timeouts", 1, CoreUtils.MEDIUM_STACK_SIZE);

    public static final String EXPORT_BLOCK_TIMEOUT_MS = "EXPORT_BLOCK_TIMEOUT_MS";
    private static final Integer s_blockTimeoutMs = Integer.getInteger(EXPORT_BLOCK_TIMEOUT_MS, 60_000);
    private static final int BACKOFF_CAP_MS = 8000;

    private final VoltExportConfig m_cfg;
    private final ExportClientBase m_exportClient;

    private AdvertisedDataSource m_ads;
    private BinaryDeque<ExportRowSchema> m_pbd;
    private BinaryDequeReader<ExportRowSchema> m_reader;

    private Pair<Long, Long> m_range = new Pair<>(0L, Long.MAX_VALUE);
    private long m_count;

    // These may be changed by the block timeout logic
    private volatile ExportDecoderBase m_edb;
    private volatile int m_decoderId = 0;
    private volatile int m_blockId = 0;

    private static class PollBlock {
        final BinaryDequeReader.Entry<ExportRowSchema> m_entry;
        final long m_start;
        final long m_last;
        final long m_count;

        PollBlock(BinaryDequeReader.Entry<ExportRowSchema> entry, long start, long count) {
            m_entry = entry;
            m_start = start;
            m_count = count;
            m_last = m_start + m_count - 1;
        }

        void release() {
            m_entry.release();
        }

        @Override
        public String toString() {
            return "[" + m_start + ", " + m_last +  ", " + m_count + "]";
        }
    }

    public ExportRunner(VoltExportConfig cfg, ExportClientBase exportClient) {
        m_cfg = cfg;
        m_exportClient = exportClient;
    }

    @Override
    public VoltExportResult call() {

        ExportSequenceNumberTracker tracker = null;
        Exception lastError = null;
        try {
            if (!parseRange()) {
                LOG.infoFmt("%s processed %d rows (skipped = %d, exported = %d), export INCOMPLETE", this, 0, 0, 0);
                return new VoltExportResult(false, tracker, m_cfg.stream_name, m_cfg.partition);
            }
            if (!m_cfg.onlyscan) {
                LOG.infoFmt("%s exporting range = [%d, %d]",
                        this, m_range.getFirst(), m_range.getSecond());
            }
            setup();

            m_reader = m_pbd.openForRead("foo");
            tracker = new ExportSequenceNumberTracker(scanForGap());
            LOG.infoFmt("%s scanned PBD: %s", this, tracker.toString());
            if (m_cfg.onlyscan) return new VoltExportResult(true, tracker, m_cfg.stream_name, m_cfg.partition);

            PollBlock pb = null;
            do {
                // Poll 1 block from PBD
                pb = pollPersistentDeque();
                if (pb == null) {
                    break;
                }

                // Process and discard polled block
                // If block incompletely processed, exit without discarding (we hit the end
                // of the range and we don't want the block release to trigger the deletion of the PBD file)
                // NOTE: this requires running against a production build that doesn't check for memory leaks
                if (!processBlock(pb)) {
                    break;
                }
                pb.release();
                pb = null;

            } while (true);
        }
        catch (Exception e) {
            LOG.errorFmt(this + "%s failed, exiting after %d rows", this, m_count);
            e.printStackTrace();
            lastError = e;
        }
        finally {
            finalizeDecoder();
        }

        // Print enough information to let the user resume after a failure - note: no range information shown
        if (lastError == null) {
            LOG.infoFmt("%s exported %d rows, export COMPLETE", this, m_count);
            return new VoltExportResult(true, tracker, m_cfg.stream_name, m_cfg.partition);
        }
        else {
            LOG.infoFmt("%s exported %d rows, export INCOMPLETE", this, m_count);
            return new VoltExportResult(false, tracker, m_cfg.stream_name, m_cfg.partition);
        }
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + ":" + m_cfg.stream_name + ":" + m_cfg.partition;
    }

    private boolean parseRange() {
        if (StringUtils.isBlank(m_cfg.range)) {
            // No range == max range
            return true;
        }

        Long start = 0L, end = 0L;
        try {
            String[] numbers = m_cfg.range.split(",");
            if (numbers.length != 2) {
                throw new IllegalArgumentException("range requires 2 numbers");
            }

            start = Long.parseLong(numbers[0]);
            end = Long.parseLong(numbers[1]);

            if (start >= end) {
                throw new IllegalArgumentException("invalid range");
            }
        }
        catch (Exception e) {
            LOG.error("Failed to parse the range...");
            e.printStackTrace();
            return false;
        }

        m_range = new Pair<Long, Long>(start, end);
        return true;
    }

    private PollBlock pollPersistentDeque() {
        PollBlock block = null;
        try {
            BinaryDequeReader.Entry<ExportRowSchema> entry = m_reader.pollEntry(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
            if (entry != null) {
                ByteBuffer b = entry.getData();
                b.order(ByteOrder.LITTLE_ENDIAN);
                long seqNo = b.getLong(StreamBlock.SEQUENCE_NUMBER_OFFSET);
                long committedSeqNo = b.getLong(StreamBlock.COMMIT_SEQUENCE_NUMBER_OFFSET);
                int tupleCount = b.getInt(StreamBlock.ROW_NUMBER_OFFSET);
                long uniqueId = b.getLong(StreamBlock.UNIQUE_ID_OFFSET);

                block = new PollBlock(entry, seqNo, tupleCount);
            }
        }
        catch (Exception e) {
            LOG.error("Failed to poll from persistent binary deque");
            e.printStackTrace();
        }
        return block;
    }

    private boolean canPoll() {
        return !Thread.currentThread().isInterrupted();
    }

    // Return true if completely processed, or false if we hit the end of the range
    private boolean processBlock(PollBlock block) throws Exception {
        int backoffQuantity = 10 + (int)(10 * ThreadLocalRandom.current().nextDouble());

        long seqNo = 0L;
        while(canPoll()) {
            m_blockId += 1;
            int decoderGeneration = m_decoderId;
            ScheduledFuture<?> blockTimeout = s_timeoutExecutor.schedule(new Runnable() {
                @Override
                public void run() {
                    handleBlockTimeout(block, m_blockId);
                }}, s_blockTimeoutMs, TimeUnit.MILLISECONDS);

            try {
                final ByteBuffer buf = block.m_entry.getData();
                buf.order(ByteOrder.LITTLE_ENDIAN);
                buf.position(StreamBlock.HEADER_SIZE);

                ExportRow row = null;
                boolean firstRowOfBlock = true;

                // Get a decoder for this attempt at decoding the block.
                // A block timeout may reset it and should generate a RestartBlockException
                ExportDecoderBase edb = getDecoder(block);

                // Process rows
                while (buf.hasRemaining() && canPoll()) {
                    int length = buf.getInt();
                    byte[] rowdata = new byte[length];
                    buf.get(rowdata, 0, length);

                    // Handle schema change
                    ExportRow schema = edb.getExportRowSchema();
                    if (schema == null || schema.generation != block.m_entry.getExtraHeader().generation) {

                        // Schema change: must be on start of a block.
                        assert firstRowOfBlock;

                        // Set the new schema used to decode rows.
                        ExportRowSchema newSchema = block.m_entry.getExtraHeader();
                        edb.setExportRowSchema(newSchema);
                    }

                    // Get the sequence number of this row
                    seqNo = seqNo == 0L ? block.m_start : seqNo + 1;

                    // handle the range
                    if (seqNo < m_range.getFirst().longValue()) {
                        continue;
                    }
                    else if (seqNo > m_range.getSecond().longValue()) {
                        break;
                    }

                    // Export row
                    row = ExportRow.decodeRow(edb.getExportRowSchema(), m_cfg.partition,
                            System.currentTimeMillis(), rowdata);

                    if (firstRowOfBlock) {
                        edb.onBlockStart(row);
                        firstRowOfBlock = false;
                    }
                    edb.processRow(row);
                    m_count++;

                    // Catch the last row of the range
                    if (seqNo == m_range.getSecond().longValue()) {
                        break;
                    }
                }

                if (row != null) {
                    edb.onBlockCompletion(row);
                }

                // Done with the block when we processed all rows
                return seqNo == block.m_last;
            }
            catch (RestartBlockException e) {
                if (!canPoll()) {
                    break;
                }
                if (e.requestBackoff) {
                    backoffQuantity = doBackoff(backoffQuantity, block);
                }
            }
            catch (Exception e) {
                /*
                 * A decoder reset may have occurred and an unexpected exception (e.g. NPE) thrown by the decoder.
                 * If this is the case, restart the block.
                 */
                if (m_decoderId == decoderGeneration) {
                    // No reset, genuine unexpected exception
                    throw e;
                }
                else if (!canPoll()) {
                    break;
                }
                else {
                    LOG.info(this + " ignores exception and restarts block: ");
                    e.printStackTrace();
                }
                backoffQuantity = doBackoff(backoffQuantity, block);
            }
            finally {
                blockTimeout.cancel(false);
            }
        }

        // Something bad happened
        return false;
    }

    private int doBackoff(int curBackoff, PollBlock block) {
        int backoff = curBackoff;
        try {
            if (backoff >= BACKOFF_CAP_MS) {
                LOG.info(this + " hits maximum restart backoff on block " + block);
            }
            else {
                LOG.info(this + " sleeping " + backoff + " seconds on " + block);
            }
            Thread.sleep(backoff);
        }
        catch (InterruptedException ignore) {}

        //Cap backoff to 8 seconds, then double modulo some randomness
        if (backoff < BACKOFF_CAP_MS) {
            backoff += (backoff * .5);
            backoff += (backoff * .5 * ThreadLocalRandom.current().nextDouble());
        }
        return backoff;
    }

    private void handleBlockTimeout(PollBlock block, int blockId) {
        if (m_blockId != blockId) {
            LOG.warn(this + " hit a spurious block timeout on block " + block
                    + ": expected " + blockId + ", got " + m_blockId);
            return;
        }

        LOG.warn(this + " hit a block timeout on block " + block + ", reset decoder");
        finalizeDecoder();
        createDecoder();
    }

    synchronized void createDecoder() {
        m_edb = m_exportClient.constructExportDecoder(m_ads);
        m_decoderId += 1;
    }

    synchronized void finalizeDecoder() {
        try {
            m_edb.sourceNoLongerAdvertised(m_ads);
        }
        catch (Exception e) {
            LOG.error(this + " failed to close decoder");
            e.printStackTrace();
        }
        m_edb = null;
    }

    synchronized ExportDecoderBase getDecoder(PollBlock block) throws RestartBlockException {
        if (m_edb == null) {
            LOG.error(this + " found no decoder for block " + block);
            throw new RestartBlockException(true);
        }
        return m_edb;
    }

    private void setup() throws IOException {

        // Create ads
        m_ads = new AdvertisedDataSource(
                m_cfg.partition,
                m_cfg.stream_name.toUpperCase(),
                null,
                System.currentTimeMillis(),
                1L,
                null,
                null,
                null,
                AdvertisedDataSource.ExportFormat.SEVENDOTX);

        m_edb = m_exportClient.constructExportDecoder(m_ads);
        String nonce = m_cfg.stream_name.toUpperCase() + "_" + m_cfg.partition;
        constructPBD(nonce);
    }

    private void constructPBD(String nonce) throws IOException {
        ExportRowSchemaSerializer serializer = new ExportRowSchemaSerializer();
        m_pbd = PersistentBinaryDeque.builder(nonce, new VoltFile(m_cfg.indir), VOLTLOG)
                .initialExtraHeader(null, serializer)
                .compression(true)
                .deleteExisting(false)
                .build();
    }

    private ExportSequenceNumberTracker scanForGap() throws IOException {
        ExportSequenceNumberTracker tracker = new ExportSequenceNumberTracker();
        m_pbd.scanEntries(new BinaryDequeScanner() {
            @Override
            public void scan(BBContainer bbc) {
                ByteBuffer b = bbc.b();
                ByteOrder endianness = b.order();
                b.order(ByteOrder.LITTLE_ENDIAN);
                final long startSequenceNumber = b.getLong();
                b.getLong(); // committedSequenceNumber
                final int tupleCount = b.getInt();
                b.order(endianness);
                tracker.addRange(startSequenceNumber, startSequenceNumber + tupleCount - 1);
            }

        });
        return tracker;
    }

}
