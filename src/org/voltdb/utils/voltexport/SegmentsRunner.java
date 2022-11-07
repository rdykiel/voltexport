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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.Callable;

import org.voltdb.catalog.Database;
import org.voltdb.export.ExportSequenceNumberTracker;
import org.voltdb.exportclient.ExportClientBase;
import org.voltdb.exportclient.ExportToFileClient;
import org.voltdb.utils.voltexport.VoltExport.VoltExportConfig;

import com.google_voltpatches.common.collect.Range;

/**
 * A class that executes a sequence of {@link ExportRunner} invocations, exporting
 * a sequence of {@link ExportSequenceNumberTracker} segments ({@link Range}).
 * <p>
 * The one {@link ExportToFileClient} instance is used per segment because we use
 * a different nonce per segment. The {@link ExportRunner} invocations must be
 * serialized to work around PBD file deletion by the export clients.
 * <p>
 * The call returns 0 for success, nonzero for errors.
 */
public class SegmentsRunner implements Callable<Integer> {

    private final int m_hostId;
    private final String m_inDir;
    private final String m_outDir;
    private final String m_name;
    private final int m_partition;
    private final ExportSequenceNumberTracker m_segments;
    private final Properties m_props;
    private final Database m_db;

    public SegmentsRunner(int hostId, String inDir, String outDir, String name, int partition,
            ExportSequenceNumberTracker segments, Properties props, Database db) {
        m_hostId = hostId;
        m_inDir = inDir;
        m_outDir = outDir;
        m_name = name;
        m_partition = partition;
        m_segments = segments;
        m_props = props;
        m_db = db;
    }


    @Override
    public Integer call() throws Exception {
        // Note: creating a different export client per segment, because of the need
        // to distinguish the nonces
        ArrayList<ExportClientBase> exportClients = new ArrayList<>();
        int errors = 0;

        try {
            VoltExportConfig cfgTemplate = new VoltExportConfig();
            cfgTemplate.exportall = false;
            cfgTemplate.indir = m_inDir;
            cfgTemplate.stream_name = m_name;
            cfgTemplate.partition = m_partition;
            cfgTemplate.onlyscan = false;

            for (Range<Long> range : m_segments.getRanges()) {
                long startSeq = ExportSequenceNumberTracker.start(range);
                long endSeq = ExportSequenceNumberTracker.end(range);
                LOG.infoFmt("Running export runner for host %d range [%d, %d]", m_hostId, startSeq, endSeq);

                VoltExportConfig cfg = (VoltExportConfig) cfgTemplate.clone();
                cfg.range = String.format("%d,%d", startSeq, endSeq);

                ExportClientBase exportClient = createExportClient(startSeq, endSeq);
                exportClients.add(exportClient);

                ExportRunner runner = new ExportRunner(cfg, exportClient, m_db);
                VoltExportResult res = runner.call();
                if (!res.success) {
                    errors += 1;
                }
            }
        }
        catch (Exception e) {
            errors += 1;
        }
        finally {
            // Shutdown clients but don't count errors
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
        return errors;
    }

    private ExportClientBase createExportClient(long startSeq, long endSeq)
            throws ClassNotFoundException, Exception {
        ExportClientBase client = new ExportToFileClient();
        client.configure(getProperties(startSeq, endSeq));
        client.setTargetName(m_name);
        return client;
    }

    private Properties getProperties(long startSeq, long endSeq) throws IOException {
        Properties properties = (Properties) m_props.clone();
        String nonce = getNonce(m_name, m_partition, startSeq, endSeq);
        properties.put("nonce", nonce);
        properties.put("outdir", m_outDir);

        return properties;
    }

    public static String getNonce(String name, int partition, long start, long end) {
        return String.format("%s_%d_%d_%d", name, partition, start, end);
    }
}
