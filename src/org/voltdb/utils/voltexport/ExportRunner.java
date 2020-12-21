package org.voltdb.utils.voltexport;

import static org.voltdb.utils.voltexport.VoltExport.LOG;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.voltcore.logging.Level;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.EstTime;
import org.voltcore.utils.RateLimitedLogger;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.export.AdvertisedDataSource;
import org.voltdb.export.StreamBlock;
import org.voltdb.exportclient.ExportClientBase;
import org.voltdb.exportclient.ExportDecoderBase;
import org.voltdb.exportclient.ExportDecoderBase.RestartBlockException;
import org.voltdb.exportclient.ExportRow;
import org.voltdb.exportclient.ExportRowSchema;
import org.voltdb.exportclient.ExportRowSchemaSerializer;
import org.voltdb.utils.BinaryDeque;
import org.voltdb.utils.BinaryDequeReader;
import org.voltdb.utils.PersistentBinaryDeque;
import org.voltdb.utils.VoltFile;
import org.voltdb.utils.voltexport.VoltExport.VoltExportConfig;

public class ExportRunner implements Runnable {
    private final RateLimitedLogger logLimitedWarn =  new RateLimitedLogger(TimeUnit.MINUTES.toMillis(1), LOG, Level.WARN);

    // Create a singleton scheduled thread pool for block processing timeouts
    private static final ScheduledThreadPoolExecutor s_timeoutExecutor =
            CoreUtils.getScheduledThreadPoolExecutor("Block Processing Timeouts", 1, CoreUtils.MEDIUM_STACK_SIZE);

    public static final String EXPORT_BLOCK_TIMEOUT_MS = "EXPORT_BLOCK_TIMEOUT_MS";
    private static final Integer s_blockTimeoutMs = Integer.getInteger(EXPORT_BLOCK_TIMEOUT_MS, 60_000);
    private static final int BACKOFF_CAP_MS = 8000;

    private final VoltExportConfig m_cfg;
    private final int m_partition;
    private final ExportClientBase m_exportClient;

    private Catalog m_catalog;
    private AdvertisedDataSource m_ads;

    private BinaryDeque<ExportRowSchema> m_pbd;
    private BinaryDequeReader<ExportRowSchema> m_reader;
    private int m_count = 0;

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

        @Override
        public String toString() {
            return "[" + m_start + ", " + m_last +  ", " + m_count + "]";
        }
    }

    public ExportRunner(VoltExportConfig cfg, int partition, ExportClientBase exportClient) {
        m_cfg = cfg;
        m_partition = partition;
        m_exportClient = exportClient;
    }

    @Override
    public void run() {
        try {
            setup();

            m_reader = m_pbd.openForRead("foo");
            PollBlock pb = null;
            do {
                // Poll 1 block from PBD
                pb = pollPersistentDeque();
                if (pb == null) {
                    break;
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug(this + " polled " + pb);
                }

                if (!processBlock(pb)) {
                    throw new RuntimeException(this + " failed block " + pb);
                }
                // Update count and discard polled block
                m_count += pb.m_count;
                pb = null;

            } while (true);

        }
        catch (Exception e) {
            LOG.error(this + " failed, exiting after " + m_count + " rows", e);
        }
        LOG.info(this + " processed " + m_count + " rows");
        System.out.println(this + " processed " + m_count + " rows");
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + ":" + m_cfg.stream_name + ":" + m_partition;
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
            LOG.error("Failed to poll from persistent binary deque", e);
        }
        return block;
    }

    private boolean canPoll() {
        return !Thread.currentThread().isInterrupted();
    }

    private boolean processBlock(PollBlock block) throws Exception {
        int backoffQuantity = 10 + (int)(10 * ThreadLocalRandom.current().nextDouble());

        while(canPoll()) {
            m_blockId += 1;
            int decoderGeneration = m_decoderId;
            ScheduledFuture<?> blockTimeout = s_timeoutExecutor.schedule(new Runnable() {
                @Override
                public void run() {
                    handleBlockTimeout(block, m_blockId);
                }}, s_blockTimeoutMs, TimeUnit.MILLISECONDS);

            try {
                long tupleCount = 0;
                final ByteBuffer buf = block.m_entry.getData();
                buf.order(ByteOrder.LITTLE_ENDIAN);
                buf.position(StreamBlock.HEADER_SIZE);

                ExportRow row = null;
                boolean firstRowOfBlock = true;

                // Get a decoder for this attempt at decoding the block.
                // A block timeout may reset it and should generate a RestartBlockException
                ExportDecoderBase edb = getDecoder(block);
                LOG.info("XXX buf position = " + buf.position() + ", limit = " + buf.limit());

                // Process rows
                while (buf.hasRemaining() && canPoll()) {
                    int length = buf.getInt();
                    byte[] rowdata = new byte[length];
                    buf.get(rowdata, 0, length);

                    ExportRow schema = edb.getExportRowSchema();
                    if (schema == null || schema.generation != block.m_entry.getExtraHeader().generation) {

                        // Schema change: must be on start of a block.
                        assert firstRowOfBlock;

                        // Set the new schema used to decode rows.
                        ExportRowSchema newSchema = block.m_entry.getExtraHeader();
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(this + " sets schema to: " + newSchema);
                        }
                        edb.setExportRowSchema(newSchema);
                    }
                    row = ExportRow.decodeRow(edb.getExportRowSchema(), m_partition,
                            System.currentTimeMillis(), rowdata);

                    if (firstRowOfBlock) {
                        edb.onBlockStart(row);
                        firstRowOfBlock = false;
                    }
                    edb.processRow(row);
                    tupleCount++;
                }

                if (row != null) {
                    edb.onBlockCompletion(row);
                }

                // This block was completely processed if we processed all tuples
                return tupleCount == block.m_count;
            }
            catch (RestartBlockException e) {
                if (!canPoll()) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(this + " ignores block restart exception when stopping polling.");
                    }
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
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(this + " ignores exception when stopping polling: ", e);
                    }
                    break;
                }
                else if (LOG.isDebugEnabled()) {
                    LOG.debug(this + " ignores exception and restarts block: ", e);
                }
                backoffQuantity = doBackoff(backoffQuantity, block);
            }
            finally {
                blockTimeout.cancel(false);
            }
        }
        return false;
    }

    private int doBackoff(int curBackoff, PollBlock block) {
        int backoff = curBackoff;
        try {
            if (backoff >= BACKOFF_CAP_MS) {
                logLimitedWarn.log(this + " hits maximum restart backoff on block " + block,
                        EstTime.currentTimeMillis());
            }
            else if (LOG.isDebugEnabled()) {
                LOG.debug(this + " sleeping " + backoff + " seconds on " + block);
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
            LOG.error(this + " failed to close decoder", e);
        }
        m_edb = null;
    }

    synchronized ExportDecoderBase getDecoder(PollBlock block) throws RestartBlockException {
        if (m_edb == null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(this + " found no decoder for block " + block);
            }
            throw new RestartBlockException(true);
        }
        return m_edb;
    }

    private void setup() throws IOException {
        // Create dummy catalog and dummy table to enable pbd creation
        m_catalog = new Catalog();
        m_catalog.execute("add / clusters cluster");
        m_catalog.execute("add /clusters#cluster databases database");
        addTable(m_cfg.stream_name);

        // Create ads
        m_ads = new AdvertisedDataSource(
                m_partition,
                m_cfg.stream_name.toUpperCase(),
                null,
                System.currentTimeMillis(),
                1L,
                null,
                null,
                null,
                AdvertisedDataSource.ExportFormat.SEVENDOTX);

        m_edb = m_exportClient.constructExportDecoder(m_ads);
        String nonce = m_cfg.stream_name.toUpperCase() + "_" + m_partition;
        constructPBD(nonce);
    }

    /*
     * BELOW hacked catalog copied from MockVoltDB
     */
    private void addTable(String tableName)
    {
        getDatabase().getTables().add(tableName);
        getTable(tableName).setIsreplicated(false);
        getTable(tableName).setSignature(tableName);
    }

    private Cluster getCluster()
    {
        return m_catalog.getClusters().get("cluster");
    }

    private Database getDatabase()
    {
        return getCluster().getDatabases().get("database");
    }

    private Table getTable(String tableName)
    {
        return getDatabase().getTables().get(tableName);
    }

    private void constructPBD(String nonce) throws IOException {
        Table streamTable = getTable(m_cfg.stream_name);

        ExportRowSchema schema = ExportRowSchema.create(streamTable, m_partition, 1, 1);
        ExportRowSchemaSerializer serializer = new ExportRowSchemaSerializer();

        m_pbd = PersistentBinaryDeque.builder(nonce, new VoltFile(m_cfg.export_overflow), LOG)
                .initialExtraHeader(schema, serializer)
                .compression(true)
                .deleteExisting(false)
                .build();
    }

}
