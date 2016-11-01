package org.hiero.sketch.storage;

import org.hiero.sketch.table.ColumnDescription;
import org.hiero.sketch.table.StringListColumn;
import org.hiero.sketch.table.api.ContentsKind;
import org.hiero.sketch.table.api.IColumn;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Reads a newline-separated text file into a string column.
 */
public class TextFileReader {
    private final Path file;
    TextFileReader(final Path filename) {
        this.file = filename;
    }

    IColumn readFile(final String columnName) throws IOException {
        final ColumnDescription desc = new ColumnDescription(columnName, ContentsKind.String, false);
        final StringListColumn result = new StringListColumn(desc);
        Files.lines(this.file).forEach(result::append);
        return result;
    }
}
