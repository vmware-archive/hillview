package org.hillview.targets;

import org.hillview.HillviewComputation;
import org.hillview.HillviewRpc;
import org.hillview.RpcRequest;
import org.hillview.RpcRequestContext;
import org.hillview.dataset.api.IDataSet;
import org.hillview.dataset.api.IMap;
import org.hillview.maps.LoadFilesMapper;
import org.hillview.sketches.FileSizeSketch;
import org.hillview.storage.IFileReference;
import org.hillview.table.PrivacySchema;
import org.hillview.table.api.ITable;
import org.hillview.table.rows.PrivacyMetadata;

import java.nio.file.Paths;
import java.util.HashMap;

public class PrivateFileDescriptionTarget extends FileDescriptionTarget {
    private String basename;

    public static final String METADATA_NAME = "metadata.json";

    private PrivacySchema metadata;

    public PrivateFileDescriptionTarget(IDataSet<IFileReference> files, HillviewComputation computation, String basename) {
        super(files, computation);

        this.basename = basename;
        this.metadata = PrivacySchema.loadFromFile(Paths.get(basename, METADATA_NAME));
    }

    @HillviewRpc
    public void getFileSize(RpcRequest request, RpcRequestContext context) {
        FileSizeSketch sk = new FileSizeSketch();
        this.runCompleteSketch(this.files, sk, (e, c) -> new AugmentedInfo(e.fileCount, e.totalSize, true),
                request, context);
    }

    /* Same as in FileDescriptionTarget, but returns a PrivateTableTarget rather than a normal TableTarget. */
    @HillviewRpc
    public void loadTable(RpcRequest request, RpcRequestContext context) {
        IMap<IFileReference, ITable> loader = new LoadFilesMapper();
        this.runMap(this.files, loader, (d, c) -> new PrivateTableTarget(d, c, metadata), request, context);
    }
}
