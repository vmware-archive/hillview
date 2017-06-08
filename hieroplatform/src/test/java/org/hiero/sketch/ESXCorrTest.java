package org.hiero.sketch;

import org.hiero.sketches.CorrMatrix;
import org.hiero.sketches.SampleCorrelationSketch;
import org.hiero.storage.CsvFileReader;
import org.hiero.table.Schema;
import org.hiero.table.api.ITable;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ESXCorrTest {
    static final String dataFolder = "../data";
    static final String csvFile = "trim_rows.csv";
    static final String schemaFile = "trim_ESX.schema";

    @Test
    public void ESXCorrTest() throws IOException {
        Path path = Paths.get(dataFolder, schemaFile);
        Schema schema = Schema.readFromJsonFile(path);
        path = Paths.get(dataFolder, csvFile);
        CsvFileReader.CsvConfiguration config = new CsvFileReader.CsvConfiguration();
        config.allowFewerColumns = true;
        config.hasHeaderRow = false;
        config.allowMissingData = true;
        config.schema = schema;
        CsvFileReader r = new CsvFileReader(path, config);
        ITable t = r.read();
        Assert.assertNotNull(t);
        List<String> cn = new ArrayList<>();
        for(int i=1; i < schema.getColumnCount(); i++)
            cn.add(schema.getColName(i));
        SampleCorrelationSketch ip = new SampleCorrelationSketch(cn, 0.1);
        CorrMatrix cm = ip.create(t);
        System.out.print(Arrays.toString(cm.getCorrelationWith(cn.get(0))));
    }
}
