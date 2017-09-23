package org.hillview.utils;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.LinkedHashMap;

public class DateParsing {
    /**
     * Used when parsing; this is set the first time when parsing a date
     * and used subsequently.
     */
    public DateTimeFormatter parserFormatter;
    /**
     * Used in conjunction with the parseFormatter.  If true
     * parse the strings as LocalDate. Java is really stupid in this respect,
     * and the spec is unclear about this.
     * @see <a href="http://stackoverflow.com/questions/27454025/unable-to-obtain-localdatetime-from-temporalaccessor-when-parsing-localdatetime">Parsing LocalDateTime</a>
     */
    public boolean parseAsDate;

    private static final LinkedHashMap<String, String> DATE_FORMAT_REGEXPS =
            new LinkedHashMap<String, String>() {{
                put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}$", "yyyy-MM-dd HH:mm:ss");
                put("^\\d{1,2}\\s[a-z]{4,}\\s\\d{4}\\s\\d{1,2}:\\d{2}:\\d{2}$", "dd MMMM yyyy HH:mm:ss");
                put("^\\d{1,2}\\s[a-z]{3}\\s\\d{4}\\s\\d{1,2}:\\d{2}:\\d{2}$", "dd MMM yyyy HH:mm:ss");
                put("^\\d{4}/\\d{1,2}/\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}$", "yyyy/MM/dd HH:mm:ss");
                put("^\\d{1,2}/\\d{1,2}/\\d{4}\\s\\d{1,2}:\\d{2}:\\d{2}$", "MM/dd/yyyy HH:mm:ss");
                put("^\\d{1,2}-\\d{1,2}-\\d{4}\\s\\d{1,2}:\\d{2}:\\d{2}$", "dd-MM-yyyy HH:mm:ss");
                put("^\\d{1,2}-\\d{1,2}-\\d{4}\\s\\d{1,2}:\\d{2}$", "dd-MM-yyyy HH:mm");
                put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}$", "yyyy-MM-dd HH:mm");
                put("^\\d{1,2}/\\d{1,2}/\\d{4}\\s\\d{1,2}:\\d{2}$", "MM/dd/yyyy HH:mm");
                put("^\\d{4}/\\d{1,2}/\\d{1,2}\\s\\d{1,2}:\\d{2}$", "yyyy/MM/dd HH:mm");
                put("^\\d{1,2}\\s[a-z]{3}\\s\\d{4}\\s\\d{1,2}:\\d{2}$", "dd MMM yyyy HH:mm");
                put("^\\d{1,2}\\s[a-z]{4,}\\s\\d{4}\\s\\d{1,2}:\\d{2}$", "dd MMMM yyyy HH:mm");
                put("^\\d{1,2}-\\d{1,2}-\\d{4}$", "dd-MM-yyyy");
                put("^\\d{4}-\\d{1,2}-\\d{1,2}$", "yyyy-MM-dd");
                put("^\\d{1,2}/\\d{1,2}/\\d{4}$", "MM/dd/yyyy");
                put("^\\d{4}/\\d{1,2}/\\d{1,2}$", "yyyy/MM/dd");
                put("^\\d{1,2}\\s[a-z]{3}\\s\\d{4}$", "dd MMM yyyy");
                put("^\\d{1,2}\\s[a-z]{4,}\\s\\d{4}$", "dd MMMM yyyy");
                put("^\\d{12}$", "yyyyMMddHHmm");
                put("^\\d{8}\\s\\d{4}$", "yyyyMMdd HHmm");
                put("^\\d{8}$", "yyyyMMdd");
                put("^\\d{14}$", "yyyyMMddHHmmss");
                put("^\\d{8}\\s\\d{6}$", "yyyyMMdd HHmmss");
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
        boolean[] asDate = {false, true};

        for (boolean b : asDate) {
            this.parseAsDate = b;
            for (DateTimeFormatter d : toTry) {
                try {
                    if (b)
                        LocalDate.parse(s, d);
                    else
                        LocalDateTime.parse(s, d);

                    HillviewLogging.logger.info(
                            "Guessed date format {}", d);
                    this.parserFormatter = d;
                    return;
                } catch (DateTimeParseException ex) {
                    continue;
                }
            }
        }

        this.parseAsDate = false;
        // none of the standard formats worked, let's try some custom ones
        for (String regexp : DATE_FORMAT_REGEXPS.keySet()) {
            if (s.toLowerCase().matches(regexp)) {
                String format = DATE_FORMAT_REGEXPS.get(regexp);
                this.parserFormatter = DateTimeFormatter.ofPattern(format);
                HillviewLogging.logger.info(
                        "Guessed date format {}", regexp);
                return;
            }
        }

        throw new RuntimeException("Could not guess parsing format for date " + s);
    }

    public LocalDateTime parse(String s) {
        LocalDateTime dt;
        if (this.parseAsDate) {
            LocalDate date = LocalDate.parse(s, this.parserFormatter);
            dt = date.atStartOfDay();
        } else {
            dt = LocalDateTime.parse(s, this.parserFormatter);
        }
        return dt;
    }
}
