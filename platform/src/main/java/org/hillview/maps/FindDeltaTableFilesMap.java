package org.hillview.maps;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;
import org.apache.hadoop.conf.Configuration;
import org.hillview.dataset.api.IMap;
import org.hillview.storage.FileSetDescription;
import org.hillview.storage.IFileReference;
import org.hillview.storage.delta.DeltaTableDescription;
import org.hillview.utils.HillviewLogger;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class FindDeltaTableFilesMap<T> implements IMap<T, List<IFileReference>> {
    static final long serialVersionUID = 1;
    private final DeltaTableDescription description;

    public FindDeltaTableFilesMap(DeltaTableDescription description) {
        this.description = description;
    }

    @Override
    public List<IFileReference> apply(@Nullable T unused) {
        HillviewLogger.instance.info("Find files for delta table",
                "path: {0}, snapshotVersion: {1}", description.path, description.snapshotVersion);

        DeltaLog log = null;
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        try {
            log = DeltaLog.forTable(conf, description.path);
        } catch (Exception e) {
            HillviewLogger.instance.error("Failed to load transaction log for delta table", e);
            return Collections.emptyList();
        }

        Snapshot snapshot = null;
        if (description.snapshotVersion == null) {
            snapshot = log.snapshot();
        } else {
            try {
                snapshot = log.getSnapshotForVersionAsOf(description.snapshotVersion);
            } catch (IllegalArgumentException e) {
                HillviewLogger.instance.error("The requested snapshot version is not available", e);
                return Collections.emptyList();
            }
        }

        if (!description.path.endsWith("/"))
            description.path += "/";

        FileSetDescription fileSetDescription = new FileSetDescription();
        fileSetDescription.fileKind = "parquet";
        return snapshot.getAllFiles()
                .stream()
                .map(addFile -> fileSetDescription.createFileReference(description.path + addFile.getPath()))
                .collect(Collectors.toList());
    }
}
