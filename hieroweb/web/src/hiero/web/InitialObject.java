package hiero.web;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.hiero.sketch.dataset.LocalDataSet;
import org.hiero.sketch.dataset.api.IJson;
import org.hiero.sketch.table.Table;
import org.hiero.sketch.table.api.ITable;

import javax.websocket.Session;

public class InitialObject extends RpcTarget {
    static class RemoteTableId implements IJson {
        public final String id;
        RemoteTableId(String id) { this.id = id; }
    }

    public InitialObject(@NonNull String objectId) {
        super(objectId);
    }

    @HieroRpc
    void loadTable(@NonNull RpcRequest request, @NonNull Session session) {
        // TODO: look at request.  Now we just supply always the same table
        String id = this.server.freshId();
        Table t = Table.testTable();
        LocalDataSet<ITable> data = new LocalDataSet<ITable>(t);
        TableTarget table = new TableTarget(id, data);
        this.server.addObject(table);

        RpcReply reply = request.createReply(new RemoteTableId(id));
        this.server.sendReply(reply, session);
        request.closeSession(session);
    }
}
