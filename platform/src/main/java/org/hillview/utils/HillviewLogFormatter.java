/*
 * Copyright (c) 2019 VMware Inc. All Rights Reserved.
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

import javax.annotation.Nullable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.LogRecord;
import java.util.logging.SimpleFormatter;

public class HillviewLogFormatter extends SimpleFormatter {
    private final String newline = System.lineSeparator();
    private final DateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
    private StringBuilder builder = new StringBuilder();

    @SuppressWarnings("ConstantConditions")
    @Override
    public synchronized String format(LogRecord record) {
        @Nullable
        HillviewLogger logger = HillviewLogger.instance;

        this.builder.setLength(0);
        String d = df.format(new Date(record.getMillis()));
        this.builder.append(d);
        this.builder.append(',');
        if (logger != null)
            this.builder.append(logger.role);
        this.builder.append(',');
        this.builder.append(record.getLevel().toString());
        this.builder.append(',');
        if (logger != null)
            this.builder.append(logger.machine);
        this.builder.append(',');
        this.builder.append(record.getMessage());
        this.builder.append(this.newline);
        return builder.toString();
    }
}
