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

import javax.annotation.Nullable;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.LinkedHashMap;
import java.util.Map;

public class DateParsing {
    /**
     * Used when parsing; this is set the first time when parsing a date
     * and used subsequently.
     */
    @Nullable
    private DateTimeFormatter parserFormatter;
    /**
     * Used in conjunction with the parseFormatter.  If true
     * parse the strings as LocalDate. Java is really stupid in this respect,
     * and the spec is unclear about this.
     * @see <a href="http://stackoverflow.com/questions/27454025/unable-to-obtain-localdatetime-from-temporalaccessor-when-parsing-localdatetime">Parsing LocalDateTime</a>
     */
    private boolean parseAsDate;

    private static final LinkedHashMap<String, String> DATE_FORMAT_REGEXPS =
            // Note that the regexp used as the key is used against the lowercased string
            new LinkedHashMap<String, String>() {{
                put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s+\\d{1,2}:\\d{2}:\\d{2}$", "yyyy-M-d H:mm:ss");
                put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s+\\d{1,2}:\\d{2}:\\d{2}.\\d{9}$",
                        "yyyy-M-d H:m:ss.SSSSSSSSS");
                put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s+\\d{1,2}:\\d{2}:\\d{2}.\\d{8}$",
                        "yyyy-M-d H:m:ss.SSSSSSSS");
                put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s+\\d{1,2}:\\d{2}:\\d{2}.\\d{7}$",
                        "yyyy-M-dd H:m:ss.SSSSSSS");
                put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s+\\d{1,2}:\\d{2}:\\d{2}.\\d{6}$",
                        "yyyy-M-d H:mm:ss.SSSSSS");
                put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s+\\d{1,2}:\\d{2}:\\d{2}.\\d{5}$",
                        "yyyy-M-d H:mm:ss.SSSSS");
                put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s+\\d{1,2}:\\d{2}:\\d{2}.\\d{4}$",
                        "yyyy-M-d H:mm:ss.SSSS");
                put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s+\\d{1,2}:\\d{2}:\\d{2},\\d{3}$",
                        "yyyy-M-d H:mm:ss,SSS");
                put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s+\\d{1,2}:\\d{2}:\\d{2}.\\d{3}$",
                        "yyyy-M-d H:mm:ss.SSS");
                put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s+\\d{1,2}:\\d{2}:\\d{2}.\\d{2}$",
                        "yyyy-M-d H:mm:ss.SS");
                put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s+\\d{1,2}:\\d{2}:\\d{2}.\\d$",
                        "yyyy-M-d H:mm:ss.S");
                put("^\\d{1,2}\\s[a-z]{4,}\\s\\d{4}\\s+\\d{1,2}:\\d{2}:\\d{2}$", "d M yyyy H:mm:ss");
                put("^\\d{1,2}\\s[a-z]{3}\\s\\d{4}\\s+\\d{1,2}:\\d{2}:\\d{2}$", "d MMM yyyy H:mm:ss");
                put("^\\d{4}/\\d{1,2}/\\d{1,2}\\s+\\d{1,2}:\\d{2}:\\d{2}$", "yyyy/M/dd H:mm:ss");
                put("^\\d{1,2}/\\d{1,2}/\\d{4}\\s+\\d{1,2}:\\d{2}:\\d{2}$", "M/d/yyyy H:mm:ss");
                put("^\\d{1,2}/\\d{1,2}/\\d{4}\\s+\\d{1,2}:\\d{2}:\\d{2}.\\d{3}$", "M/d/yyyy H:mm:ss.SSS");
                put("^\\d{1,2}-\\d{1,2}-\\d{4}\\s+\\d{1,2}:\\d{2}:\\d{2}$", "d-M-yyyy H:mm:ss");
                put("^\\d{1,2}-\\d{1,2}-\\d{4}\\s+\\d{1,2}:\\d{2}$", "d-M-yyyy H:mm");
                put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s+\\d{1,2}:\\d{2}$", "yyyy-M-d H:mm");
                put("^\\d{1,2}/\\d{1,2}/\\d{4}\\s+\\d{1,2}:\\d{2}$", "M/d/yyyy H:mm");
                put("^\\d{4}/\\d{1,2}/\\d{1,2}\\s+\\d{1,2}:\\d{2}$", "yyyy/M/d H:mm");
                put("^\\d{1,2}\\s+[a-z]{3}\\s+\\d{4}\\s\\d{1,2}:\\d{2}$", "d MMM yyyy H:mm");
                put("^\\d{1,2}\\s+[a-z]{4,}\\s+\\d{4}\\s\\d{1,2}:\\d{2}$", "d MMMM yyyy H:mm");
                put("^[a-z]{3}\\s+\\d{1,2}\\s+\\d{1,2}:\\d{2}$", "MMM d H:mm");
                put("^[a-z]{3}\\s+\\d{1,2}\\s+\\d{1,2}:\\d{2}:\\d{2}$", "MMM d H:mm:ss");
                put("^\\d{1,2}-\\d{1,2}-\\d{4}$", "d-M-yyyy");
                put("^\\d{4}-\\d{1,2}-\\d{1,2}$", "yyyy-M-d");
                put("^\\d{1,2}/\\d{1,2}/\\d{4}$", "M/d/yyyy");
                put("^\\d{4}/\\d{1,2}/\\d{1,2}$", "yyyy/M/d");
                put("^\\d{1,2}\\s+[a-z]{3}\\s+\\d{4}$", "d MMM yyyy");
                put("^\\d{1,2}\\s+[a-z]{4,}\\s+\\d{4}$", "d MMMM yyyy");
                put("^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.\\d{6}", "yyyy-mm-ddTHH:mm:ss.SSSSSS");
            }};

    private static final DateTimeFormatter[] toTry = {
            DateTimeFormatter.BASIC_ISO_DATE,
            DateTimeFormatter.ISO_LOCAL_DATE,
            DateTimeFormatter.ISO_OFFSET_DATE,
            DateTimeFormatter.ISO_DATE,
            DateTimeFormatter.ISO_LOCAL_TIME,
            DateTimeFormatter.ISO_OFFSET_TIME,
            DateTimeFormatter.ISO_TIME,
            DateTimeFormatter.ISO_LOCAL_DATE_TIME,
            DateTimeFormatter.ISO_OFFSET_DATE_TIME,
            DateTimeFormatter.ISO_ZONED_DATE_TIME,
            DateTimeFormatter.ISO_DATE_TIME,
            DateTimeFormatter.ISO_ORDINAL_DATE,
            DateTimeFormatter.ISO_INSTANT,
            DateTimeFormatter.RFC_1123_DATE_TIME
    };

    @SuppressWarnings("UnnecessaryContinue")
    public DateParsing(String s) {
        s = Utilities.singleSpaced(s);
        boolean[] asDate = {false, true};

        for (boolean b : asDate) {
            this.parseAsDate = b;
            //noinspection ForLoopReplaceableByForEach
            for (int i=0; i < toTry.length; i++) {
                DateTimeFormatter d = toTry[i];
                try {
                    if (b)
                        LocalDate.parse(s, d);
                    else
                        LocalDateTime.parse(s, d);
                    HillviewLogger.instance.info("Guessed date format", "{0}", d);
                    this.parserFormatter = d;
                    return;
                } catch (DateTimeParseException ex) {
                    continue;
                }
            }
        }

        this.parseAsDate = false;
        // none of the standard formats worked, let's try some custom ones
        for (Map.Entry<String, String> regexpEntry : DATE_FORMAT_REGEXPS.entrySet()) {
            if (s.toLowerCase().matches(regexpEntry.getKey())) {
                String format = regexpEntry.getValue();
                this.parserFormatter = //DateTimeFormatter.ofPattern(format);
                        new DateTimeFormatterBuilder()
                        .appendPattern(format)
                                // We need this because some patterns have no year.
                        .parseDefaulting(ChronoField.YEAR_OF_ERA, ZonedDateTime.now().getYear())
                        .toFormatter()
                        .withZone(ZoneId.systemDefault());
                HillviewLogger.instance.info("Guessed date format", "{0}", regexpEntry.getKey());
                return;
            }
        }
        throw new RuntimeException("Could not guess parsing format for date " + s);
    }


    public Instant parse(String s) {
        s = Utilities.singleSpaced(s);
        Converters.checkNull(this.parserFormatter);
        if (this.parseAsDate) {
            return LocalDate.parse(s, this.parserFormatter)
                    .atStartOfDay(ZoneId.systemDefault())
                    .toInstant();
        } else {
            return LocalDateTime.parse(s, this.parserFormatter)
                    .atZone(ZoneId.systemDefault())
                    .toInstant();
        }
    }
}
