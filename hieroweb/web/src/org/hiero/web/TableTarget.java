import org.checkerframework.checker.nullness.qual.NonNull;
import org.hiero.sketch.table.api.ContentsKind;
import org.hiero.sketch.view.ColumnDescriptionView;
import org.hiero.sketch.view.RowView;
import org.hiero.sketch.view.TableDataView;

import javax.websocket.Session;

public class TableTarget extends RpcTarget {
    public TableTarget(@NonNull String objectId) {
        super(objectId);
    }

    @Override
    void execute(@NonNull RpcRequest request, @NonNull Session session) {
        ColumnDescriptionView c0 = new ColumnDescriptionView(ContentsKind.String, "Name", 1);
        ColumnDescriptionView c1 = new ColumnDescriptionView(ContentsKind.Int, "Age", -2);
        RowView rv0 = new RowView(4, new Object[] { "Mike", 20 });
        RowView rv1 = new RowView(3, new Object[] { "John", 30 });
        RowView rv2 = new RowView(1, new Object[] { "Tom", 5 });
        TableDataView tdv = new TableDataView(new ColumnDescriptionView[] { c0, c1 }, 10, 0, new RowView[] { rv0, rv1, rv2 });

        RpcReply reply = request.createReply(tdv.toJson());
        this.server.sendReply(reply, session);

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {}

        RowView rv3 = new RowView(2, new Object[] { "Jake", 40 });
        tdv = new TableDataView(new ColumnDescriptionView[] { c0, c1 }, 10, 0,
                new RowView[] { rv0, rv1, rv2, rv3 });

        reply = request.createReply(tdv.toJson());
        this.server.sendReply(reply, session);
    }
}
