package hiero.web;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.hiero.sketch.dataset.LocalDataSet;
import org.hiero.sketch.table.Table;
import org.hiero.sketch.table.api.ITable;

import javax.websocket.Session;

public class InitialObject extends RpcTarget {
    @HieroRpc
    void loadTable(@NonNull RpcRequest request, @NonNull Session session) {
        // TODO: look at request.  Now we just supply always the same table
        Table t = Table.testTable();
        LocalDataSet<ITable> data = new LocalDataSet<ITable>(t);
        TableTarget table = new TableTarget(data);
        RpcObjectManager.instance.addObject(table);

        RpcReply reply = request.createReply(table.idToJson());
        reply.send(session);
        request.closeSession(session);
    }

    @Override
    public String toString() {
        return "Initial object, " + super.toString();
    }
}
