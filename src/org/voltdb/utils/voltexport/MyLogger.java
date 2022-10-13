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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.voltcore.logging.Level;

public class MyLogger {
    static final SimpleDateFormat LOG_DF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");

    // Ad-hoc rate limiting
    private ConcurrentHashMap<Level, Long> m_lastLogs = new ConcurrentHashMap<>();

    void error(String msg) {
        log(Level.ERROR, msg);
    }
    void errorFmt(String format, Object... args) {
        log(Level.ERROR, format, args);
    }
    void warn(String msg) {
        log(Level.WARN, msg);
    }
    void warnFmt(String format, Object... args) {
        log(Level.WARN, format, args);
    }
    void info(String msg) {
        log(Level.INFO, msg);
    }
    void infoFmt(String format, Object... args) {
        log(Level.INFO, format, args);
    }

    void rateLimitedLog(long suppressInterval, Level level, String format, Object... args) {
        long now = System.nanoTime();
        long last = m_lastLogs.getOrDefault(level, 0L);
        if (TimeUnit.NANOSECONDS.toSeconds(now - last) > suppressInterval) {
            m_lastLogs.put(level, now);
            log(level, format, args);
        }
    }

    private void log(Level level, String format, Object... args) {
        log(level, String.format(format, args));
    }

    // Synchronized to allow logging from multiple threads - not concerned about performance
    private synchronized void log(Level level, String msg) {
        System.out.print(LOG_DF.format(new Date()));
        System.out.println(String.format(" %s: %s", level, msg));
    }
}
