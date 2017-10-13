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

import org.apache.commons.lang.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;

public class HillviewLogger implements IHillviewLogger {
    private final Logger logger;
    private final String machine;
    // This can be null if not initialized, but I am not using the Nullable annotation
    // so I don't have to check everywhere.
    @SuppressWarnings("NullableProblems")
    public static HillviewLogger instance;

    private HillviewLogger(String filename) {
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.showThreadName", "true");
        System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "yyyy-MM-dd HH:mm:ss.SSS");
        System.setProperty("org.slf4j.simpleLogger.logFile", filename);
        this.logger = LoggerFactory.getLogger("Hillview");
        this.machine = Utilities.getHostName();
    }

    public static void initialize(String filename) {
        instance = new HillviewLogger(filename);
    }

    @Override
    public void info(String message, String format, Object... arguments) {
        if (!this.logger.isInfoEnabled())
            return;
        String text = this.createMessage(message, format, arguments);
        this.logger.info(text);
    }

    @Override
    public void warn(String message, String format, Object... arguments) {
        if (!this.logger.isWarnEnabled())
            return;
        String text = this.createMessage(message, format, arguments);
        this.logger.warn(text);
    }

    @Override
    public void debug(String message, String format, Object... arguments) {
        if (!this.logger.isDebugEnabled())
            return;
        String text = this.createMessage(message, format, arguments);
        this.logger.debug(text);
    }

    @Override
    public void error(String message, Throwable ex) {
        String text = this.createMessage(message, "{0}", Utilities.throwableToString(ex));
        this.logger.error(text);
    }

    @Override
    public void error(String message, String format, Object... arguments) {
        String text = this.createMessage(message, format, arguments);
        this.logger.error(text);
    }

    protected String createMessage(String message, String format, Object... arguments) {
        if (message.contains(","))
            throw new RuntimeException("Format message should contain no commas: " + message);
        String text = MessageFormat.format(format, arguments);
        StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
        StackTraceElement caller = stackTraceElements[3];
        String quoted = this.quote(text);
        return String.join(",", this.machine, caller.getClassName(), caller.getMethodName(),
                message, quoted);
    }

    protected String quote(String message) {
        message = message.replace("\n", "\\n");
        return StringEscapeUtils.escapeCsv(message);
    }
}
