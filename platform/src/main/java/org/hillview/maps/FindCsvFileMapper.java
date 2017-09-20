package org.hillview.maps;

import org.hillview.dataset.api.Empty;
import org.hillview.dataset.api.IMap;
import org.hillview.utils.CsvFileObject;

import javax.annotation.Nullable;
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
 * Scans a folder and finds files matching a pattern.
 * Creates a CsvFileObject for each file that matches.
 * It can contain an optional schema name.
 */
public class FindCsvFileMapper implements IMap<Empty, List<CsvFileObject>> {
    private final String folder;
    private final int maxCount;
    @Nullable
    private final String fileNamePattern;
    @Nullable
    private final String schemaFileName;

    /**
     * Create an object to find and create CsvFileObjects.
     * @param folder   Folder where files are sought.
     * @param maxCount Maximum number of files to find.  If 0 there is no limit.
     * @param fileNamePattern  Regex for file names to search.  If null all file names match.
     * @param schemaFileName   Name of schema file to search.  If null no schema is provided.
     */
    public FindCsvFileMapper(String folder, int maxCount,
                             @Nullable String fileNamePattern,
                             @Nullable String schemaFileName) {
        this.folder = folder;
        this.maxCount = maxCount;
        this.fileNamePattern = fileNamePattern;
        this.schemaFileName = schemaFileName;
    }

    @Override
    public List<CsvFileObject> apply(Empty empty) {
        Path currentRelativePath = Paths.get("");
        String cwd = currentRelativePath.toAbsolutePath().toString();
        System.out.println("Current directory is: " + cwd);

        Path folder = Paths.get(this.folder);
        Path schemaPath = this.schemaFileName != null ?
                Paths.get(this.folder, this.schemaFileName) : null;
        final List<CsvFileObject> result = new ArrayList<CsvFileObject>();

        Stream<Path> files;
        try {
            files = Files.walk(folder, 1, FileVisitOption.FOLLOW_LINKS);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        files = files.filter(f -> {
            String filename = f.getFileName().toString();
            if (fileNamePattern == null)
                return true;
            return filename.matches(this.fileNamePattern);
        });
        if (this.maxCount > 0)
            files = files.limit(this.maxCount);
        files.sorted(Comparator.comparing(Path::toString))
                .forEach(f -> {
                    CsvFileObject cfo = new CsvFileObject(f, schemaPath);
                    result.add(cfo);
                });

        if (result.size() == 0)
            throw new RuntimeException("No such files");
        return result;
    }
}
