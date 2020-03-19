package org.hillview.targets;

import org.hillview.HillviewComputation;
import org.hillview.HillviewRpc;
import org.hillview.RpcRequest;
import org.hillview.RpcRequestContext;
import org.hillview.table.PrivacySchema;
import org.hillview.dataset.api.IDataSet;
import org.hillview.dataset.api.IMap;
import org.hillview.maps.LoadFilesMap;
import org.hillview.sketches.FileSizeSketch;
import org.hillview.storage.IFileReference;
import org.hillview.table.api.ITable;

public class PrivateFileDescriptionTarget extends FileDescriptionTarget {
    static final long serialVersionUID = 1;

    private PrivacySchema metadata;
    private String schemaFilename;

    PrivateFileDescriptionTarget(IDataSet<IFileReference> files, HillviewComputation computation, String file) {
        super(files, computation);
        this.metadata = PrivacySchema.loadFromFile(file);
        this.schemaFilename = file;
    }

    @HillviewRpc
    public void getFileSize(RpcRequest request, RpcRequestContext context) {
        FileSizeSketch sk = new FileSizeSketch();
        this.runSketch(this.files, sk, request, context);
    }

    @HillviewRpc
    public void loadTable(RpcRequest request, RpcRequestContext context) {
        IMap<IFileReference, ITable> loader = new LoadFilesMap();
        this.runMap(this.files, loader, (d, c) -> new PrivateTableTarget(d, c, metadata, schemaFilename), request, context);
    }
}
