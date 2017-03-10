package org.hiero.sketch.spreadsheet;

import org.hiero.sketch.table.api.IColumn;
import org.hiero.sketch.table.api.IMembershipSet;
import org.hiero.sketch.table.api.IRowIterator;
import org.hiero.sketch.table.api.IStringConverter;

import javax.annotation.Nullable;

public class BasicColStat {
    private final int MOMENT_NUM;
    private double min;
    private Object minObject;
    private double max;
    private Object maxObject;
    private double moments[];
    private long size;

    public BasicColStat(int momentNum) {
        if (momentNum < 1)
            throw new IllegalArgumentException("number of moments cannot be positive");
        this.MOMENT_NUM = momentNum;
        this.moments = new double[this.MOMENT_NUM];
        this.min = Double.POSITIVE_INFINITY;
        this.max = Double.NEGATIVE_INFINITY;
    }

    public BasicColStat() {
        this.MOMENT_NUM = 2;
        this.moments = new double[this.MOMENT_NUM];
        this.min = Double.POSITIVE_INFINITY;
        this.max = Double.NEGATIVE_INFINITY;
    }

    public double getMin() { return this.min; }

    public Object getMinObject() { return this.minObject; }

    public double getMax() { return this.max; }

    public Object getMaxObject() { return this.maxObject; }

    /**
     *
     * @param i a number in {1,2,...,MOMENT_NUM}
     * @return the i'th moment which is the sum of x^i
     */
    public double getMoment(int i) {
        if ((i < 1) || (i > this.MOMENT_NUM))
            throw new IllegalArgumentException("moment requested doesn't exist");
        return this.moments[i - 1];
    }

    public long getSize() { return this.size; }

    public void createStats (final IColumn column, final IMembershipSet membershipSet,
    @Nullable
    final IStringConverter converter) {
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
                if (this.MOMENT_NUM > 0) {
                    double tmpMoment = val;
                    double alpha = (double) this.size / (double) (this.size + 1);
                    double beta = 1.0 - alpha;
                    this.moments[0] = (alpha * this.moments[0]) + (beta * val);
                    for (int i = 1; i < this.MOMENT_NUM; i++) {
                        tmpMoment = tmpMoment * val;
                        this.moments[i] = (alpha * this.moments[i]) + (beta * tmpMoment);
                    }
                }
                this.size++;
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
        result.size = this.size + otherStat.size;
        if (result.size > 0) {
            double alpha = (double) this.size / ((double) result.size);
            double beta = 1.0 - alpha;
            for (int i = 0; i < this.MOMENT_NUM; i++)
                result.moments[i] = (alpha * this.moments[i]) + (beta * otherStat.moments[i]);
        }
        return result;
    }
}
