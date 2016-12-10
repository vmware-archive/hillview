package org.hiero.sketch.dataset.api;

/**
 * This is a Monoid of values of type PartialResult[IDataSet[S]]
 * @param <S> Type of data in the data set.
 */
public class PRDataSetMonoid<S> extends PartialResultMonoid<IDataSet<S>> {
    public PRDataSetMonoid() {
        super(new OptionMonoid<IDataSet<S>>());
    }
}
