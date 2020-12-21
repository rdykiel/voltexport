package org.voltdb.utils.voltexport;

import static org.voltdb.utils.voltexport.VoltExport.LOG;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.export.AdvertisedDataSource;
import org.voltdb.export.StreamBlock;
import org.voltdb.exportclient.ExportClientBase;
import org.voltdb.exportclient.ExportDecoderBase;
import org.voltdb.exportclient.ExportRowSchema;
import org.voltdb.exportclient.ExportRowSchemaSerializer;
import org.voltdb.utils.BinaryDeque;
import org.voltdb.utils.BinaryDequeReader;
import org.voltdb.utils.PersistentBinaryDeque;
import org.voltdb.utils.VoltFile;
import org.voltdb.utils.voltexport.VoltExport.VoltExportConfig;

public class ExportRunner implements Runnable {

    private final VoltExportConfig m_cfg;
    private final int m_partition;
    private final ExportClientBase m_exportClient;

    private Catalog m_catalog;
    private AdvertisedDataSource m_ads;
    private ExportDecoderBase m_edb;

    private BinaryDeque<ExportRowSchema> m_pbd;
    private BinaryDequeReader<ExportRowSchema> m_reader;

    private int m_count = 0;

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
                pb = pollPersistentDeque();
                if (pb == null) {
                    break;
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug(this + " polled " + + pb.m_count + " rows at ["
                            + pb.m_start + ", " + pb.m_last + "]");
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
