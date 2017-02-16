package hiero.web;

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

    public TableTarget(@NonNull String objectId, IDataSet<ITable> table) {
        super(objectId);
        this.table = table;
    }

    @HieroRpc
    void getSchema(@NonNull RpcRequest request, @NonNull Session session) {
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
    void getTableView(@NonNull RpcRequest request, @NonNull Session session) {
    }
}
