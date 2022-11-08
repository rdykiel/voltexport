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
import static org.voltdb.utils.voltexport.VoltExport.VOLTLOG;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashSet;
import java.util.NavigableMap;
import java.util.Set;

import org.voltcore.utils.Pair;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.PbdSegmentName;
import org.voltdb.utils.PbdSegmentName.Result;

public class ExportFileVisitor implements FileVisitor<Path>{
    private final String m_indir;
    private final NavigableMap<String, Table> m_streams;

    private boolean rootVisited = false;
    private Table m_table = null;
    private int m_partition = -1;
    private boolean m_pbdFound = false;
    private Set<Pair<String, Integer>> m_results = new HashSet<>();

    public ExportFileVisitor(String indir, Database db) {
        m_indir = indir;
        m_streams = CatalogUtil.getAllStreamsExcludingViews(db);
    }

    public Set<Pair<String, Integer>> visit() throws IOException {
        m_results.clear();
        Files.walkFileTree(new File(m_indir).toPath(), this);
        return m_results;
    }

    @Override
    public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
        if (!rootVisited) {
            rootVisited = true;
        } else if (m_table == null) {
            if (!findStream(dir.getFileName().toString())) {
                LOG.infoFmt("Skipping directory for export stream which doesn't exist: %s", dir);
                return FileVisitResult.SKIP_SUBTREE;
            }
        } else if (m_partition == -1) {
            m_partition = Integer.parseInt(dir.getFileName().toString());
            m_pbdFound = false;
        } else {
            LOG.warnFmt("Unexpected directory encountered while recovering export streams: %s", dir);
            return FileVisitResult.SKIP_SUBTREE;
        }
        return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        if (m_table != null && m_partition != -1) {
            PbdSegmentName pbdName = PbdSegmentName.parseFile(VOLTLOG, file.toFile());
            if (pbdName.m_result == Result.OK) {
                m_pbdFound = true;
                if (m_table != null) {
                    if (!getPathForExportStream(m_indir, m_table.getTypeName().toUpperCase(), m_partition)
                            .equals(file.getParent().toString())) {
                        LOG.warnFmt("Misplaced export file %s", file.toAbsolutePath());
                    }
                    else {
                        m_results.add(Pair.of(m_table.getTypeName(), m_partition));
                    }
                }
                return FileVisitResult.SKIP_SIBLINGS;
            }
            else if (pbdName.m_result == Result.NOT_PBD) {
                LOG.warnFmt("%s is not a PBD file.", file);
            }
            else if (pbdName.m_result == Result.INVALID_NAME) {
                LOG.warnFmt("%s doesn't have valid PBD name.", file);
            }
        } else {
            LOG.warnFmt("Unexpected file encountered while recovering export streams: %s", file);
        }
        return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
        throw new IOException("Error visiting: " + file, exc);
    }

    @Override
    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
        if (m_partition != -1) {
            if (!m_pbdFound) {
                LOG.infoFmt("Found empty export directory: %s", dir);
            }
            m_partition = -1;
        } else {
            m_table = null;
        }
        return FileVisitResult.CONTINUE;
    }

    private boolean findStream(String streamName) {
        m_table = m_streams.get(streamName);
        return m_table != null;
    }

    public static String getPathForExportStream(String dirName, String name, int partition) throws IOException {
        Path path = Paths.get(dirName, name.toUpperCase(), Integer.toString(partition));
        if (!Files.isDirectory(path)) {
            Files.deleteIfExists(path);
            Files.createDirectories(path);
        }
        return path.toString();
    }
}
