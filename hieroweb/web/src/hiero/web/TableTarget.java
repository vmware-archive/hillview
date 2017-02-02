package hiero.web;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.hiero.sketch.dataset.api.IDataSet;
import org.hiero.sketch.table.Table;
import org.hiero.sketch.table.api.ContentsKind;
import org.hiero.sketch.view.ColumnDescriptionView;
import org.hiero.sketch.view.RowView;
import org.hiero.sketch.view.TableDataView;

import javax.websocket.Session;

public class TableTarget extends RpcTarget {
    protected final IDataSet<Table> table;

    public TableTarget(@NonNull String objectId, IDataSet<Table> table) {
        super(objectId);
        this.table = table;
    }

    static class TableViewRequest {
        public ColumnDescriptionView[] schema;
        public int rowCount;
    }

    @HieroRpc
    void getTableView(@NonNull RpcRequest request, @NonNull Session session) {
        TableViewRequest cols = gson.fromJson(request.arguments, TableViewRequest.class);
        // TODO
    }

    @HieroRpc
    void mockTable(@NonNull RpcRequest request, @NonNull Session session)
            throws InterruptedException {
        ColumnDescriptionView c0 = new ColumnDescriptionView(ContentsKind.String, "Name", 1);
        ColumnDescriptionView c1 = new ColumnDescriptionView(ContentsKind.Int, "Age", -2);
        RowView rv0 = new RowView(4, new Object[] { "Mike", 20 });
        RowView rv1 = new RowView(3, new Object[] { "John", 30 });
        RowView rv2 = new RowView(1, new Object[] { "Tom", 5 });
        TableDataView tdv = new TableDataView(new ColumnDescriptionView[] { c0, c1 }, 10, 0, new RowView[] { rv0, rv1, rv2 });

        RpcReply reply = request.createReply(tdv);
        this.server.sendReply(reply, session);

        Thread.sleep(1000);

        RowView rv3 = new RowView(2, new Object[] { "Jake", 40 });
        tdv = new TableDataView(new ColumnDescriptionView[] { c0, c1 }, 10, 0,
                new RowView[] { rv0, rv1, rv2, rv3 });

        reply = request.createReply(tdv);
        this.server.sendReply(reply, session);
    }
}
