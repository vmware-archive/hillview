package org.hiero;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.hiero.sketch.dataset.api.IDataSet;
import org.hiero.sketch.dataset.api.PartialResult;
import org.hiero.sketch.dataset.api.PartialResultMonoid;
import org.hiero.sketch.spreadsheet.SummarySketch;
import org.hiero.sketch.table.api.ITable;
import rx.Observable;

import javax.websocket.Session;

public class TableTarget extends RpcTarget {
    protected final IDataSet<ITable> table;

    public TableTarget(IDataSet<ITable> table) {
        this.table = table;
    }

    @HieroRpc
    void getSchema(RpcRequest request, Session session) {
        SummarySketch ss = new SummarySketch();
        Observable<PartialResult<SummarySketch.TableSummary>> sketches = this.table.sketch(ss);
        PartialResultMonoid<SummarySketch.TableSummary> prm =
                new PartialResultMonoid<SummarySketch.TableSummary>(ss);
        Observable<PartialResult<SummarySketch.TableSummary>> accum = sketches.scan(prm::add);
        ResultObserver<SummarySketch.TableSummary> ro =
                new ResultObserver<SummarySketch.TableSummary>(request, session);
        accum.subscribe(ro);
    }

    @HieroRpc
    void getTableView(RpcRequest request, Session session) {
        // TODO
    }

    @Override
    public String toString() {
        return "TableTarget object, " + super.toString();
    }
}
