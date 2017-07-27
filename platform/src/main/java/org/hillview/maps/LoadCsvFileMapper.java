package org.hillview.maps;

import org.hillview.dataset.api.Empty;
import org.hillview.dataset.api.IMap;
import org.hillview.utils.CsvFileObject;

import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

/**
 * Only used for the flight data demo.
 */
public class LoadCsvFileMapper implements IMap<Empty, List<CsvFileObject>> {
    private final int which;

    public LoadCsvFileMapper(final int which) {
        this.which = which;
    }

    @SuppressWarnings("RedundantIfStatement")
    @Override
    public List<CsvFileObject> apply(Empty empty) {
        Path currentRelativePath = Paths.get("");
        String cwd = currentRelativePath.toAbsolutePath().toString();
        System.out.println("Current directory is: " + cwd);

        String dataFolder = "../data/";
        String smallFileSchema = "On_Time.schema";
        String smallFile = "On_Time_Sample.csv";
        String schemaFile = "short.schema";

        String vropsFile = "vrops.csv";
        String vropsSchema = "vrops.schema";

        Path schemaPath = Paths.get(dataFolder, schemaFile);

        final List<CsvFileObject> result = new ArrayList<CsvFileObject>();
        if (this.which >= 0 && this.which <= 1) {
            int limit = this.which == 0 ? 12 : 1;
            Path folder = Paths.get(dataFolder);
            Stream<Path> files;
            try {
                files = Files.walk(folder, 1, FileVisitOption.FOLLOW_LINKS);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            files.filter(f -> {
                String filename = f.getFileName().toString();
                if (filename.endsWith(".csv") && (filename.startsWith("19") || filename.startsWith("20"))) return true;
                return false;
            }).limit(limit)
                    .sorted(Comparator.comparing(Path::toString))
                    .forEach(f -> {
                        CsvFileObject cfo = new CsvFileObject(f, schemaPath);
                        result.add(cfo);
                        System.out.println("Added " + this.toString());
                    });
            if (result.size() == 0) {
                throw new RuntimeException("No such files");
            }
        } else if (this.which == 2) {
            CsvFileObject file = new CsvFileObject(
                    Paths.get(dataFolder, smallFile),
                    Paths.get(dataFolder, smallFileSchema));
            result.add(file);
        } else if (this.which == 3) {
            CsvFileObject file = new CsvFileObject(
                    Paths.get(dataFolder, vropsFile),
                    Paths.get(dataFolder, vropsSchema));
            result.add(file);
        } else {
            System.out.println("About to throw unexepcted file");
            throw new RuntimeException("Unexpected file");
        }
        return result;
    }
}
