package org.hillview.sketches;

import org.hillview.dataset.api.ISketch;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * Base class for centroid sketches. It computes the centroids of a set of partitions of the data, in a space defined
 * by the specified column names.
 * The partitions are defined by a (bi)function that returns a key indicating the partition.
 */
public class CentroidsSketch<T> implements ISketch<ITable, Centroids<T>> {
    private final List<String> columnNames;
    private final BiFunction<ITable, Integer, T> keyFunc;

    /**
     * @param keyFunc A function that returns the partition key of a row. It defines how the data is partitioned.
     * @param columnNames List of columns that define the space in which we compute the centroids.
     */
    public CentroidsSketch(BiFunction<ITable, Integer, T> keyFunc, List<String> columnNames) {
        this.keyFunc = keyFunc;
        this.columnNames = columnNames;
    }

    @Override
    public Centroids<T> create(ITable data) {
        return new Centroids<T>(
                data.getMembershipSet(),
                i -> this.keyFunc.apply(data, i),
                this.columnNames.stream().map(data::getColumn).collect(Collectors.toList())
        );
    }

    @Override
    public Centroids<T> zero() {
        return new Centroids<T>();
    }

    @Override
    public Centroids<T> add(@Nullable Centroids<T> left, @Nullable Centroids<T> right) {
        return Converters.checkNull(left).union(Converters.checkNull(right));
    }
}
