package org.hiero.sketches;

import org.hiero.dataset.api.IJson;
import org.hiero.table.api.IColumn;
import org.hiero.table.api.IMembershipSet;
import org.hiero.table.api.IRowIterator;
import org.hiero.table.api.IStringConverter;
import javax.annotation.Nullable;

/**
 * A class that scans a column and collects basic statistics: maximum, minimum,
 * number of non-empty rows and the moments of asDouble values.
 */
public class BasicColStats implements IJson {
    private final int momentCount;
    // Number of values that have been used to compute the stats.
    private long rowCount;
    // The following values are meaningful only if rowCount > 0.
    private double min;
    @Nullable private Object minObject;
    private double max;
    @Nullable private Object maxObject;
    private final double moments[];


    public BasicColStats(int momentCount) {
        if (momentCount < 0)
            throw new IllegalArgumentException("number of moments cannot be negative");
        this.momentCount = momentCount;
        this.moments = new double[this.momentCount];
        this.min = 0;  // we cannot use infinity, since that cannot be serialized as JSON
        this.max = 0;
        this.rowCount = 0;
    }

    public double getMin() { return this.min; }
    @Nullable
    public Object getMinObject() { return this.minObject; }

    public double getMax() { return this.max; }
    @Nullable
    public Object getMaxObject() { return this.maxObject; }
    /**
     *
     * @param i Moment number; note that moments are numbered from 1, not 0.
     * @return the i'th moment: the normalized sum of x^i
     */
    public double getMoment(int i) {
        return this.moments[i - 1];
    }

    /**
     * @return the number of non-empty rows in a column
     */
    public long getRowCount() { return this.rowCount; }

    public void createStats(final IColumn column, final IMembershipSet membershipSet,
                            @Nullable final IStringConverter converter) {
        final IRowIterator myIter = membershipSet.getIterator();
        int currRow = myIter.getNextRow();
        while (currRow >= 0) {
            if (!column.isMissing(currRow)) {
                double val = column.asDouble(currRow, converter);
                if (this.rowCount == 0) {
                    this.min = val;
                    this.max = val;
                    this.minObject = column.getObject(currRow);
                    this.maxObject = this.minObject;
                } else if (val < this.min) {
                    this.min = val;
                    this.minObject = column.getObject(currRow);
                } else if (val > this.max) {
                    this.max = val;
                    this.maxObject = column.getObject(currRow);
                }
                if (this.momentCount > 0) {
                    double tmpMoment = val;
                    double alpha = (double) this.rowCount / (double) (this.rowCount + 1);
                    double beta = 1.0 - alpha;
                    this.moments[0] = (alpha * this.moments[0]) + (beta * val);
                    for (int i = 1; i < this.momentCount; i++) {
                        tmpMoment = tmpMoment * val;
                        this.moments[i] = (alpha * this.moments[i]) + (beta * tmpMoment);
                    }
                }
                this.rowCount++;
            }
            currRow = myIter.getNextRow();
        }
    }

    public BasicColStats union(final BasicColStats otherStat) {
        BasicColStats result = new BasicColStats(this.momentCount);
        if (this.rowCount == 0)
            return otherStat;
        if (otherStat.rowCount == 0)
            return this;

        if (this.min < otherStat.min) {
            result.min = this.min;
            result.minObject = this.minObject;
        } else {
            result.min = otherStat.min;
            result.minObject = otherStat.minObject;
        }

        if (this.max > otherStat.max) {
            result.max = this.max;
            result.maxObject = this.maxObject;
        } else {
            result.max = otherStat.max;
            result.maxObject = otherStat.maxObject;
        }
        result.rowCount = this.rowCount + otherStat.rowCount;
        if (result.rowCount > 0) {
            double alpha = (double) this.rowCount / ((double) result.rowCount);
            double beta = 1.0 - alpha;
            for (int i = 0; i < this.momentCount; i++)
                result.moments[i] = (alpha * this.moments[i]) + (beta * otherStat.moments[i]);
        }
        return result;
    }
}
