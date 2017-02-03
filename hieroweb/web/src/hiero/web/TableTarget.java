package hiero.web;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.hiero.sketch.dataset.api.IDataSet;
import org.hiero.sketch.dataset.api.PartialResult;
import org.hiero.sketch.spreadsheet.NextKList;
import org.hiero.sketch.spreadsheet.TopKSketch;
import org.hiero.sketch.table.api.ITable;
import org.hiero.sketch.table.RecordOrder;
import org.hiero.sketch.table.api.ContentsKind;
import org.hiero.sketch.view.ColumnDescriptionView;
import org.hiero.sketch.view.RowView;
import org.hiero.sketch.view.TableDataView;
import rx.Observable;
import rx.Observer;

import javax.websocket.Session;

public class TableTarget extends RpcTarget {
    protected final IDataSet<ITable> table;

    public TableTarget(@NonNull String objectId, IDataSet<ITable> table) {
        super(objectId);
        this.table = table;
    }

    static class TableViewRequest {
        public ColumnDescriptionView[] schema;
        public int rowCount;

        RecordOrder getSortOrder() {
            RecordOrder ro = new RecordOrder();
            for (ColumnDescriptionView cd : schema)
                ro.append(cd.toOrientation());
            return ro;
        }
    }

    class ViewObserver implements Observer<PartialResult<NextKList>> {
        @NonNull final RpcRequest request;
        @NonNull final Session session;

        ViewObserver(@NonNull RpcRequest request, @NonNull Session session) {
            this.request = request;
            this.session = session;
        }

        @Override
        public void onCompleted() {
            this.request.closeSession(this.session);
        }

        @Override
        public void onError(Throwable throwable) {
            RpcReply reply = this.request.createReply(throwable);
            TableTarget.this.server.sendReply(reply, this.session);
        }

        @Override
        public void onNext(PartialResult<NextKList> pr) {
            //RpcReply reply = this.request.createReply(pr);
            //TableTarget.this.server.sendReply(reply, this.session);
        }
    }

    @HieroRpc
    void getTableView(@NonNull RpcRequest request, @NonNull Session session) {
        TableViewRequest req = gson.fromJson(request.arguments, TableViewRequest.class);

        TopKSketch sk = new TopKSketch(req.getSortOrder(), req.rowCount);
        Observable<PartialResult<NextKList>> sketch = this.table.sketch(sk);
        sketch.subscribe(new ViewObserver(request, session));
    }

    @HieroRpc
    void mockTable(@NonNull RpcRequest request, @NonNull Session session)
            throws InterruptedException {
        ColumnDescriptionView c0 = new ColumnDescriptionView(ContentsKind.String, "Name", false, 1);
        ColumnDescriptionView c1 = new ColumnDescriptionView(ContentsKind.Int, "Age", false, -2);
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
