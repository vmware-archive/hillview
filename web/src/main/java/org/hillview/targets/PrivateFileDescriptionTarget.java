package org.hillview.targets;

import org.hillview.HillviewComputation;
import org.hillview.HillviewRpc;
import org.hillview.RpcRequest;
import org.hillview.RpcRequestContext;
import org.hillview.dataset.api.IDataSet;
import org.hillview.dataset.api.IMap;
import org.hillview.maps.LoadFilesMapper;
import org.hillview.sketches.FileSizeSketch;
import org.hillview.storage.FileSetDescription;
import org.hillview.storage.IFileReference;
import org.hillview.table.PrivacySchema;
import org.hillview.table.api.ITable;

import java.nio.file.Paths;

public class PrivateFileDescriptionTarget extends FileDescriptionTarget {
    private PrivacySchema metadata;

    public PrivateFileDescriptionTarget(IDataSet<IFileReference> files, HillviewComputation computation, String basename) {
        super(files, computation);

        this.metadata = PrivacySchema.loadFromFile(Paths.get(basename, FileSetDescription.PRIVACY_METADATA_NAME));
    }

    @HillviewRpc
    public void getFileSize(RpcRequest request, RpcRequestContext context) {
        FileSizeSketch sk = new FileSizeSketch();
        this.runCompleteSketch(this.files, sk, (e, c) -> new PrivateFileSizeInfo(e.fileCount, e.totalSize, true),
                request, context);
    }

    @HillviewRpc
    public void loadTable(RpcRequest request, RpcRequestContext context) {
        IMap<IFileReference, ITable> loader = new LoadFilesMapper();
        this.runMap(this.files, loader, (d, c) -> new PrivateTableTarget(d, c, metadata), request, context);
    }
}
