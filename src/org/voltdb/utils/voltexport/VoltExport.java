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
import java.util.Set;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.Pair;
import org.voltdb.CLIConfig;

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

    static VoltExportConfig s_cfg = new VoltExportConfig();

    File m_indir;
    File m_outdir;

    public static void main(String[] args) throws IOException {
        s_cfg.parse(VoltExport.class.getName(), args);

        VoltExport ve = new VoltExport();
        ve.run();
    }

    void run() throws IOException {
        m_indir = new File(s_cfg.export_overflow);
        if (!m_indir.canRead()) {
            throw new IOException("Cannot read input directory " + m_indir.getAbsolutePath());
        }

        m_outdir = new File(s_cfg.out_dir);
        if (!m_outdir.canWrite()) {
            throw new IOException("Cannot write output directory " + m_outdir.getAbsolutePath());
        }

        File files[] = m_indir.listFiles();
        if (files == null || files.length == 0) {
            throw new IOException("No files in input directory " + m_indir.getAbsolutePath());
        }

        Set<Integer> partitions = new HashSet<>();
        for (File data: files) {
            if (data.getName().endsWith(".pbd")) {
                System.out.println("XXX file = " + data.getName());
                // Note: PbdSegmentName#parseFile bugs here
                Pair<String, Integer> topicPartition = getTopicPartition(data.getName());
                System.out.println("XXX topicPartition = " + topicPartition);
                if (s_cfg.stream_name.equalsIgnoreCase(topicPartition.getFirst())) {
                    partitions.add(topicPartition.getSecond());
                }
                else {
                    LOG.info("Ignoring " + data.getName()) ;
                }
            }
        }
        System.out.println("XXX Partitions for " + s_cfg.stream_name + " = " + partitions);

        ArrayList<Thread> threads = new ArrayList<>(partitions.size());
        /*
        partitions.forEach(p -> threads.add(new Thread(new Runnable() {
            @Override
            public void run() {
                // TODO Auto-generated method stub
                try {
                System.out.println("XXX thread " + p + " sleeping 10s...");
                Thread.sleep(10_000);
                } catch (Exception ignore) {
                }

            }})));
            */
        partitions.forEach(p -> threads.add(new Thread(new ExportRunner(s_cfg, p))));

        threads.forEach(t -> t.start());
        threads.forEach(t -> {try {t.join(); } catch(Exception e) {}});
        System.out.println("XXX DONE " + threads.size() + " threads...");
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
}
