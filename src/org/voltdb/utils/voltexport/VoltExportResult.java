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

import org.voltdb.export.ExportSequenceNumberTracker;

/**
 * This class is just a bag of information returned by {@link ExportRunner}.
 */
public class VoltExportResult {
    public final boolean success;
    public final ExportSequenceNumberTracker tracker;
    public final String stream_name;
    public final int partition;

    public VoltExportResult(boolean success, ExportSequenceNumberTracker tracker, String stream_name, int partition) {
        this.success = success;
        this.tracker = tracker;
        this.stream_name = stream_name;
        this.partition = partition;
    }
}
