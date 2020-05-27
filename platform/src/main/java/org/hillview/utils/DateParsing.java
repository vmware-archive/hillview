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
    
    enum ParseKind {
        DateNoZone,   // No time component
        DateWithZone, // No time component, but with time zone
        DateTimeNoZone,  // Date-time, no time zone component
        DateTimeWithZone // Date-time with time zone
    }
    
    ParseKind kind;
    
    private static final LinkedHashMap<String, String> DATE_FORMAT_REGEXPS =
            new LinkedHashMap<String, String>() {{
                // Note that the regexp used as the key is used against the lowercased string
                put("^\\d{1,2}-\\d{1,2}-\\d{4}$", "d-M-yyyy");
                put("^\\d{1,2}/\\d{1,2}/\\d{4}$", "M/d/yyyy");
                put("^\\d{4}/\\d{1,2}/\\d{1,2}$", "yyyy/M/d");
                put("^\\d{1,2}\\s+[a-z]{3}\\s+\\d{4}$", "d MMM yyyy");
                put("^\\d{1,2}\\s+[a-z]{4,}\\s+\\d{4}$", "d MMMM yyyy");
                put("^\\d{4}-\\d{1,2}-\\d{1,2}$", "yyyy-M-d");
            }};
    // These format expressions contain timezone information
    private static final LinkedHashMap<String, String> TIMEZOME_FORMAT_REGEXPS =
            new LinkedHashMap<String, String>() {{
                put("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2} [+-]\\d{4} \\w+", "yyyy-M-dd HH:mm:ss Z z");
            }};
    private static final LinkedHashMap<String, String> DATETIME_FORMAT_REGEXPS =
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
                put("^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.\\d{6}", "yyyy-M-ddTHH:mm:ss.SSSSSS");
            }};

    private static final LinkedHashMap<DateTimeFormatter, ParseKind> standardFormats =
            new LinkedHashMap<DateTimeFormatter, ParseKind>() {{
                put(DateTimeFormatter.BASIC_ISO_DATE, ParseKind.DateNoZone);
                put(DateTimeFormatter.ISO_LOCAL_DATE, ParseKind.DateNoZone);
                put(DateTimeFormatter.ISO_OFFSET_DATE, ParseKind.DateWithZone);
                put(DateTimeFormatter.ISO_DATE, ParseKind.DateWithZone);
                put(DateTimeFormatter.ISO_LOCAL_DATE_TIME, ParseKind.DateTimeNoZone);
                put(DateTimeFormatter.ISO_OFFSET_DATE_TIME, ParseKind.DateTimeNoZone);
                put(DateTimeFormatter.ISO_ZONED_DATE_TIME, ParseKind.DateTimeWithZone);
                put(DateTimeFormatter.ISO_DATE_TIME, ParseKind.DateTimeWithZone);
                put(DateTimeFormatter.ISO_ORDINAL_DATE, ParseKind.DateNoZone);
                put(DateTimeFormatter.ISO_INSTANT, ParseKind.DateTimeNoZone);
                put(DateTimeFormatter.RFC_1123_DATE_TIME, ParseKind.DateTimeWithZone);
            }};

    @SuppressWarnings("UnnecessaryContinue")
    public DateParsing(String s) {
        s = Utilities.singleSpaced(s);
        for (Map.Entry<DateTimeFormatter, ParseKind> f : standardFormats.entrySet()) {
            DateTimeFormatter d = f.getKey();
            try {
                switch (f.getValue()) {
                    case DateNoZone:
                    case DateWithZone:
                        LocalDate.parse(s, d);
                        break;
                    case DateTimeNoZone:
                    case DateTimeWithZone:
                        LocalDateTime.parse(s, d);
                        break;
                }
                HillviewLogger.instance.info("Guessed date format", "{0}", d);
                this.parserFormatter = d;
                this.kind = f.getValue();
                return;
            } catch (DateTimeParseException ex) {
                continue;
            }
        }

        // none of the standard formats worked, let's try some custom ones
        // First we try formats that specify a timezone
        this.kind = ParseKind.DateTimeWithZone;
        for (Map.Entry<String, String> regexpEntry : TIMEZOME_FORMAT_REGEXPS.entrySet()) {
            if (s.toLowerCase().matches(regexpEntry.getKey())) {
                String format = regexpEntry.getValue();
                this.parserFormatter = new DateTimeFormatterBuilder()
                        .appendPattern(format)
                        .parseDefaulting(ChronoField.YEAR_OF_ERA, ZonedDateTime.now().getYear())
                        .toFormatter();
                HillviewLogger.instance.info("Guessed date format", "{0}", regexpEntry.getKey());
                return;
            }
        }

        for (ParseKind b : Utilities.list(ParseKind.DateNoZone, ParseKind.DateTimeNoZone)) {
            this.kind = b;
            LinkedHashMap<String, String> map;
            if (b == ParseKind.DateNoZone)
                map = DATE_FORMAT_REGEXPS;
            else
                map = DATETIME_FORMAT_REGEXPS;
            for (Map.Entry<String, String> regexpEntry : map.entrySet()) {
                if (s.toLowerCase().matches(regexpEntry.getKey())) {
                    String format = regexpEntry.getValue();
                    this.parserFormatter = new DateTimeFormatterBuilder()
                            .appendPattern(format)
                            // We need this because some patterns have no year.
                            .parseDefaulting(ChronoField.YEAR_OF_ERA, ZonedDateTime.now().getYear())
                            .toFormatter()
                            .withZone(ZoneId.systemDefault());
                    HillviewLogger.instance.info("Guessed date format", "{0}", regexpEntry.getKey());
                    return;
                }
            }
        }
        throw new RuntimeException("Could not guess parsing format for date " + s);
    }


    public Instant parse(String s) {
        s = Utilities.singleSpaced(s);
        Converters.checkNull(this.parserFormatter);
        switch (this.kind) {
            case DateNoZone:
            case DateWithZone: // TODO we are ingnoring the offset for this case.
                return LocalDate.parse(s, this.parserFormatter)
                        .atStartOfDay(ZoneId.systemDefault())
                        .toInstant();
            case DateTimeNoZone:
                return LocalDateTime.parse(s, this.parserFormatter)
                        .atZone(ZoneId.systemDefault())
                        .toInstant();
            case DateTimeWithZone:
                return ZonedDateTime.parse(s, this.parserFormatter)
                        .toInstant();
            default:
                throw new HillviewException("Unexpected datetime format" + this.kind);
        }
    }
}
