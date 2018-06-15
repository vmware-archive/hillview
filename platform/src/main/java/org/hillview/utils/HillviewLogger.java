/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hillview.utils;

import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.JdkLoggerFactory;
import org.apache.commons.lang.StringEscapeUtils;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.*;

/**
 * Core API for logging in hillview.
 * The hillview logging keeps each log message on a single line.
 * It uses commas to separate the fields.
 * The result produced by format and arguments is escaped: quotes and newlines are escaped.
 * This is a typical log message format
2017-10-12 02:18:29.084,worker,INFO,ubuntu,pool-1-thread-1,org.hillview.maps.FindCsvFileMapper,apply,Find files in folder,/hillview/data
_______date_______________who__level_machine___thread___________class__________________________method__message______________args_________
 */
public class HillviewLogger {
    // Actual logging channel.
    private final Logger logger;
    // Machine where the core is running.
    private final String machine;
    // Role of machine (worker or web server).
    private final String role;
    // Default logger if users forget to initialize
    public static HillviewLogger instance = new HillviewLogger("none", null);

    private static final Logger GRPC_LOGGER;
    private static final Logger NETTY_LOGGER;

    static {
        InternalLoggerFactory.setDefaultFactory(JdkLoggerFactory.INSTANCE);
        GRPC_LOGGER = Logger.getLogger("io.grpc");
        GRPC_LOGGER.setLevel(Level.WARNING);
        NETTY_LOGGER = Logger.getLogger("io.grpc.netty.NettyServerHandler");
        NETTY_LOGGER.setLevel(Level.WARNING);
    }

    /**
     * Create a Hillview logger.
     * @param role      Who is doing the logging: web server, worker, test, etc.
     * @param filename  File where logs are to be written.  If null logs will be written to the
     *                  console.
     */
    private HillviewLogger(String role, @Nullable String filename) {
        // Disable all default logging
        LogManager.getLogManager().reset();
        this.logger = Logger.getLogger("Hillview");
        this.machine = this.checkCommas(Utilities.getHostName());
        this.role = this.checkCommas(role);
        this.logger.setLevel(Level.INFO);

        Formatter form = new SimpleFormatter() {
            final String[] components = new String[5];
            final String newline = System.lineSeparator();
            private final DateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
            @Override
            public synchronized String format(LogRecord record) {
                this.components[0] = HillviewLogger.this.checkCommas(
                        df.format(new Date(record.getMillis())));
                this.components[1] = HillviewLogger.this.role;
                this.components[2] = HillviewLogger.this.checkCommas(
                        record.getLevel().toString());
                this.components[3] = HillviewLogger.this.machine;
                this.components[4] = record.getMessage();
                String result = String.join(",", components);
                return result + this.newline;
            }
        };

        Handler handler;
        if (filename != null) {
            try {
                handler = new FileHandler(filename);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            handler = new ConsoleHandler();
        }
        handler.setFormatter(form);
        logger.addHandler(handler);
        File currentDirectory = new File(new File(".").getAbsolutePath());
        this.info("Starting logger", "Working directory: {0}", currentDirectory);
    }

    public static void initialize(String role, @Nullable String filename) {
        instance = new HillviewLogger(role, filename);
    }

    public void setLogLevel(final Level level) {
        this.logger.setLevel(level);
    }

    @SuppressWarnings("unused")
    public void shutdown() {
        Handler[] hs = this.logger.getHandlers();
        for (Handler h : hs)
            h.close();
    }

    public void info(String message, String format, Object... arguments) {
        if (!this.logger.isLoggable(Level.INFO))
            return;
        String text = this.createMessage(message, format, arguments);
        this.logger.info(text);
    }

    public void warn(String message, String format, Object... arguments) {
        if (!this.logger.isLoggable(Level.WARNING))
            return;
        String text = this.createMessage(message, format, arguments);
        this.logger.warning(text);
    }

    private void debug(String message, String format, Object... arguments) {
        if (!this.logger.isLoggable(Level.INFO))
            return;
        String text = this.createMessage(message, format, arguments);
        this.logger.log(Level.INFO, text);
    }

    public void error(String message, Throwable ex) {
        String text = this.createMessage(message, "{0}", Utilities.throwableToString(ex));
        this.logger.severe(text);
    }

    public void error(String message, String format, Object... arguments) {
        String text = this.createMessage(message, format, arguments);
        this.logger.severe(text);
    }

    private String checkCommas(String str) {
        if (str.contains(",")) {
            HillviewLogger.instance.error("Log message should contain no commas", "{0}", str);
            str = str.replace(",", ";");
        }
        return str;
    }

    private String createMessage(String message, String format, Object... arguments) {
        message = this.checkCommas(message);
        String text = MessageFormat.format(format, arguments);
        Thread current = Thread.currentThread();
        StackTraceElement[] stackTraceElements = current.getStackTrace();
        StackTraceElement caller = stackTraceElements[3];
        String quoted = this.quote(text);
        return String.join(",", current.getName(), caller.getClassName(),
                caller.getMethodName(), message, quoted);
    }

    private String quote(String message) {
        message = message.replace("\n", "\\n");
        return StringEscapeUtils.escapeCsv(message);
    }

    public void info(String message) { this.info(message, ""); }

    public void warn(String message) { this.warn(message, ""); }

    public void debug(String message) { this.debug(message, ""); }

    public void error(String message) { this.error(message, ""); }
}
