package org.voltdb.utils.voltexport;

import static org.voltdb.utils.voltexport.VoltExport.LOG;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.export.StreamBlock;
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

    private Catalog m_catalog;
    private BinaryDeque<ExportRowSchema> m_pbd;
    private BinaryDequeReader<ExportRowSchema> m_reader;

    public ExportRunner(VoltExportConfig cfg, int partition) {
        m_cfg = cfg;
        m_partition = partition;
    }

    @Override
    public void run() {
        try {
            setup();
            String nonce = m_cfg.stream_name.toUpperCase() + "_" + m_partition;
            constructPBD(nonce);
            m_reader = m_pbd.openForRead("foo");
            StreamBlock sb = null;
            do {
                sb = pollPersistentDeque();
                if (sb != null) {
                    System.out.println(this + " polled [" + sb.startSequenceNumber() + ", "
                            + sb.lastSequenceNumber() + ", " + sb.rowCount());
                }
            } while (sb != null);

        }
        catch (Exception e) {
            LOG.error(this + " failed, exiting", e);
        }
    }

    @Override
    public String toString() {
        return this.getClass().getName() + ":" + m_cfg.stream_name + ":" + m_partition;
    }

    private StreamBlock pollPersistentDeque() {

        BinaryDequeReader.Entry<ExportRowSchema> entry = null;
        StreamBlock block = null;
        try {
            entry = m_reader.pollEntry(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
            if (entry != null) {
                ByteBuffer b = entry.getData();
                b.order(ByteOrder.LITTLE_ENDIAN);
                long seqNo = b.getLong(StreamBlock.SEQUENCE_NUMBER_OFFSET);
                long committedSeqNo = b.getLong(StreamBlock.COMMIT_SEQUENCE_NUMBER_OFFSET);
                int tupleCount = b.getInt(StreamBlock.ROW_NUMBER_OFFSET);
                long uniqueId = b.getLong(StreamBlock.UNIQUE_ID_OFFSET);

                block = new StreamBlock(entry,
                        seqNo,
                        committedSeqNo,
                        tupleCount,
                        uniqueId,
                        true);
            }
        }
        catch (Exception e) {
            LOG.error("Failed to poll from persistent binary deque", e);
        }
        return block;
    }

    /*
     * BELOW hacked catalog
     */
    private void setup() {
        m_catalog = new Catalog();
        m_catalog.execute("add / clusters cluster");
        m_catalog.execute("add /clusters#cluster databases database");
        addTable(m_cfg.stream_name);
    }

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
