package org.hillview;

import org.hillview.storage.CsvFileReader;
import org.hillview.storage.CsvFileWriter;
import org.hillview.table.HashSubSchema;
import org.hillview.table.Schema;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;
import org.hillview.utils.TestTables;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

/**
 * TODO: delete this class
 * This entry point is only used for preparing some data files for a demo.
 * It takes files named like data/On_Time_On_Time_Performance_*_*.csv and
 * removes some columns from them.  Optionally, it can also split these into
 * smaller files each.
 */
public class DemoDataCleaner {
    static final String dataFolder = "../data";
    static final String csvFile = "On_Time_Sample.csv";
    static final String schemaFile = "On_Time.schema";

    public static void main(String[] args) throws IOException {
        String[] columns = {
                "DayOfWeek", "FlightDate", "UniqueCarrier",
                "Origin", "OriginCityName", "OriginState", "Dest", "DestState",
                "DepTime", "DepDelay", "ArrTime", "ArrDelay", "Cancelled",
                "ActualElapsedTime", "Distance"
        };

        System.out.println("Splitting files in folder " + dataFolder);
        Path path = Paths.get(dataFolder, schemaFile);
        Schema schema = Schema.readFromJsonFile(path);
        HashSubSchema subSchema = new HashSubSchema(columns);
        Schema proj = schema.project(subSchema);
        proj.writeToJsonFile(Paths.get(dataFolder, "short.schema"));

        // If non-zero, split each table into parts of this size.
        final int splitSize = 0; // 1 << 16;

        String prefix = "On_Time_On_Time_Performance";
        Path folder = Paths.get(dataFolder);
        Stream<Path> files = Files.walk(folder, 1);
        files.filter(f -> {
            String filename = f.getFileName().toString();
            if (!filename.endsWith("csv")) return false;
            //noinspection RedundantIfStatement
            if (!filename.startsWith(prefix)) return false;
            return true;
        }).sorted(Comparator.comparing(Path::toString))
                .forEach(f -> {
                    String filename = f.getFileName().toString();
                    String end = filename.substring(prefix.length() + 1);
                    CsvFileReader.CsvConfiguration config = new CsvFileReader.CsvConfiguration();
                    config.allowFewerColumns = false;
                    config.hasHeaderRow = true;
                    config.allowMissingData = false;
                    config.schema = schema;
                    CsvFileReader r = new CsvFileReader(f, config);

                    ITable tbl = null;
                    try {
                        System.out.println("Reading " + f);
                        tbl = r.read();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    Converters.checkNull(tbl);

                    ITable p = tbl.project(proj);

                    //noinspection ConstantConditions
                    if (splitSize > 0) {
                        List<ITable> pieces = TestTables.splitTable(p, splitSize);

                        int index = 0;
                        for (ITable t : pieces) {
                            String baseName = end.substring(0, end.lastIndexOf("."));
                            String name = baseName + "-" + Integer.toString(index) + ".csv";
                            Path outPath = Paths.get(dataFolder, name);
                            CsvFileWriter writer = new CsvFileWriter(outPath);
                            try {
                                System.out.println("Writing " + outPath);
                                writer.writeTable(t);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                            index++;
                        }
                    } else {
                        Path outpath = Paths.get(dataFolder, end);
                        CsvFileWriter writer = new CsvFileWriter(outpath);
                        try {
                            System.out.println("Writing " + outpath);
                            writer.writeTable(p);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                });
    }
}
