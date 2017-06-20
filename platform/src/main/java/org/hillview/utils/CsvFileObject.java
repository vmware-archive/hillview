package org.hillview.utils;

import org.hillview.storage.CsvFileReader;
import org.hillview.table.Schema;
import org.hillview.table.api.ITable;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;


public class CsvFileObject {
    private final Path dataPath;
    @Nullable
    private final Path schemaPath;

    public CsvFileObject(Path path, @Nullable Path schema) {
        this.dataPath = path;
        this.schemaPath = schema;
    }

    public ITable loadTable() throws IOException {
        Schema schema = null;
        if (this.schemaPath != null) {
            String s = new String(Files.readAllBytes(this.schemaPath));
            schema = Schema.fromJson(s);
        }

        CsvFileReader.CsvConfiguration config = new CsvFileReader.CsvConfiguration();
        config.allowFewerColumns = false;
        config.hasHeaderRow = true;
        config.allowMissingData = false;
        config.schema = schema;
        CsvFileReader r = new CsvFileReader(this.dataPath, config);

        ITable tbl = r.read();
        return Converters.checkNull(tbl);
    }

    @Override
    public String toString() {
        return "CsvFile " + this.dataPath.toString();
    }
}
