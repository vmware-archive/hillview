package org.hillview.maps;

import org.hillview.dataset.api.IMap;
import org.hillview.dataset.api.Pair;
import org.hillview.table.Table;
import org.hillview.table.api.IMembershipSet;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;

public class SetOperationMap implements IMap<Pair<ITable, ITable>, ITable> {
    private final String operation;

    public SetOperationMap(String operation) {
        this.operation = operation;
    }

    @Override
    public ITable apply(Pair<ITable, ITable> data) {
        IMembershipSet first = Converters.checkNull(data.first).getMembershipSet();
        IMembershipSet second = Converters.checkNull(data.second).getMembershipSet();
        IMembershipSet rows;
        switch (this.operation) {
            case "Union":
                rows = first.union(second);
                break;
            case "Intersection":
                rows = first.intersection(second);
                break;
            case "Replace":
                rows = second;
                break;
            case "Exclude":
                rows = first.setMinus(second);
                break;
            default:
                throw new RuntimeException("Unexpected operation " + this.operation);
        }

        return new Table(data.first.getColumns(), rows);
    }
}
