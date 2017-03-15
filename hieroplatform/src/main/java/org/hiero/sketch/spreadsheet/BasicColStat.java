package org.hiero.sketch.spreadsheet;

import org.hiero.sketch.table.api.IColumn;
import org.hiero.sketch.table.api.IMembershipSet;
import org.hiero.sketch.table.api.IRowIterator;
import org.hiero.sketch.table.api.IStringConverter;
import javax.annotation.Nullable;

/**
 * A class that scans a column and collects basic statistics: maximum, minimum,
 * number of non-empty rows and the moments of asDouble values.
 */
public class BasicColStat {
    private final int momentNum;
    private double min;
    @Nullable private Object minObject;
    private double max;
    @Nullable private Object maxObject;
    private final double moments[];
    private long rowCount;

    public BasicColStat(int momentNum) {
        if (momentNum < 0)
            throw new IllegalArgumentException("number of moments cannot be negative");
        this.momentNum = momentNum;
        this.moments = new double[this.momentNum];
        this.min = Double.POSITIVE_INFINITY;
        this.max = Double.NEGATIVE_INFINITY;
    }

    public BasicColStat() {
        this.momentNum = 2;
        this.moments = new double[this.momentNum];
        this.min = Double.POSITIVE_INFINITY;
        this.max = Double.NEGATIVE_INFINITY;
    }

    public double getMin() { return this.min; }
    @Nullable
    public Object getMinObject() { return this.minObject; }

    public double getMax() { return this.max; }
    @Nullable
    public Object getMaxObject() { return this.maxObject; }
    /**
     *
     * @param i a number in {1,...,momentNum}
     * @return the i'th moment which is the sum of x^i
     */
    public double getMoment(int i) {
        if ((i < 1) || (i > this.momentNum))
            throw new IllegalArgumentException("moment requested doesn't exist");
        return this.moments[i - 1];
    }

    /**
     * @return the number of non-empty rows in a column
     */
    public long getRowCount() { return this.rowCount; }

    public void createStats (final IColumn column, final IMembershipSet membershipSet,
                             @Nullable final IStringConverter converter) {
        if (this.min < Double.POSITIVE_INFINITY)
            throw new IllegalStateException("can't create stats more than once");
        final IRowIterator myIter = membershipSet.getIterator();
        int currRow = myIter.getNextRow();
        while (currRow >= 0) {
            if (!column.isMissing(currRow)) {
                double val = column.asDouble(currRow, converter);
                if (val < this.min) {
                    this.min = val;
                    this.minObject = column.getObject(currRow);
                }
                if (val > this.max) {
                    this.max = val;
                    this.maxObject = column.getObject(currRow);
                }
                if (this.momentNum > 0) {
                    double tmpMoment = val;
                    double alpha = (double) this.rowCount / (double) (this.rowCount + 1);
                    double beta = 1.0 - alpha;
                    this.moments[0] = (alpha * this.moments[0]) + (beta * val);
                    for (int i = 1; i < this.momentNum; i++) {
                        tmpMoment = tmpMoment * val;
                        this.moments[i] = (alpha * this.moments[i]) + (beta * tmpMoment);
                    }
                }
                this.rowCount++;
            }
            currRow = myIter.getNextRow();
        }
    }

    public BasicColStat union(final BasicColStat otherStat) {
        BasicColStat result = new BasicColStat();
        if (this.min < otherStat.min) {
            result.min = this.min;
            result.minObject = this.minObject;
        }
        else {
            result.min = otherStat.min;
            result.minObject = otherStat.minObject;
        }
        if (this.max > otherStat.max) {
            result.max = this.max;
            result.maxObject = this.maxObject;
        }
        else {
            result.max = otherStat.max;
            result.maxObject = otherStat.maxObject;
        }
        result.rowCount = this.rowCount + otherStat.rowCount;
        if (result.rowCount > 0) {
            double alpha = (double) this.rowCount / ((double) result.rowCount);
            double beta = 1.0 - alpha;
            for (int i = 0; i < this.momentNum; i++)
                result.moments[i] = (alpha * this.moments[i]) + (beta * otherStat.moments[i]);
        }
        return result;
    }
}
