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
    Path file;
    TextFileReader(Path filename) {
        this.file = filename;
    }

    IColumn readFile(String columnName) throws IOException {
        ColumnDescription desc = new ColumnDescription(columnName, ContentsKind.String, false);
        StringListColumn result = new StringListColumn(desc);
        Files.lines(this.file).forEach(l -> result.append(l));
        return result;
    }
}
