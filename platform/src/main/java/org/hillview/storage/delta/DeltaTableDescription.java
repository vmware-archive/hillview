package org.hillview.storage.delta;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This class contains necessary information to access a delta table
 */
public class DeltaTableDescription implements Serializable {
    static final long serialVersionUID = 1;

    public String path;
    @Nullable
    public Long snapshotVersion;

    public List<String> getFiles() {
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        DeltaLog log = DeltaLog.forTable(conf, path);
        Snapshot snapshot = null;
        if (snapshotVersion == null) {
            snapshot = log.snapshot();
        } else {
            snapshot = log.getSnapshotForVersionAsOf(snapshotVersion);
        }

        String sep = path.endsWith("/") ? "" : "/";
        return snapshot.getAllFiles()
                .stream()
                .map(addFile -> path + sep + addFile.getPath())
                .collect(Collectors.toList());
    }

    @Override
    public String toString() {
        return "DeltaTableDescription{" +
                "path='" + path + '\'' +
                ", snapshotVersion=" + snapshotVersion +
                '}';
    }
}
