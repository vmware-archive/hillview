package org.hillview.maps;

import org.hillview.dataset.api.IMap;
import org.hillview.storage.FileSetDescription;
import org.hillview.storage.IFileReference;
import org.hillview.utils.HillviewLogger;
import org.hillview.utils.Utilities;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ExtractWorkerFilesMap<T> implements IMap<T, List<IFileReference>> {
    static final long serialVersionUID = 1;
    private final FileSetDescription fileSetDescription;
    private final Map<String, List<String>> filesPerWorker;

    public ExtractWorkerFilesMap(FileSetDescription fileSetDescription, Map<String, List<String>> filesPerWorker) {
        this.fileSetDescription = fileSetDescription;
        this.filesPerWorker = filesPerWorker;
    }

    @Override
    public List<IFileReference> apply(@Nullable T unused) {
        HillviewLogger.instance.info("Extract worker files", "{0}", filesPerWorker);
        final String hostname = Utilities.getHostName();

        if (filesPerWorker.containsKey(hostname)) {
            return filesPerWorker.get(hostname)
                    .stream()
                    .map(filePath -> fileSetDescription.createFileReference(filePath))
                    .collect(Collectors.toList());
        } else {
            HillviewLogger.instance.warn("No files assigned to this worker", "hostname: {0}", hostname);
            return Collections.emptyList();
        }
    }
}
