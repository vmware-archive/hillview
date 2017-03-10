package org.hiero.sketch;

import org.hiero.sketch.dataset.LocalDataSet;
import org.hiero.sketch.spreadsheet.*;
import org.hiero.sketch.table.SmallTable;
import org.hiero.sketch.table.api.ITable;
import org.junit.Test;
import static org.hiero.sketch.TableTest.getIntTable;

/**
 * Test class for performance profiling of histogram and CDF
 */
public class CDFTest {
    private final BasicColStat colStat;
    private final LocalDataSet<ITable> dataSet;
    private final String colName;
    private final int bigSize;

    public CDFTest () {
        this.bigSize = 100000000;
        final int numCols = 1;
        final SmallTable bigTable = getIntTable(this.bigSize, numCols);
        this.colName = bigTable.getSchema().getColumnNames().iterator().next();
        this.dataSet = new LocalDataSet<ITable>(bigTable);
        this.colStat =
               this.dataSet.blockingSketch(new BasicColStatSketch(this.colName, null));

    }

    Histogram1DLight prepareCDF(int width, int height, boolean useSampling) {
        BucketsDescriptionEqSize bDec  =
                new BucketsDescriptionEqSize(this.colStat.getMin(), this.colStat.getMax(), width);
        double sampleSize  =  2 * height * height * width;
        double rate = sampleSize / this.colStat.getSize();
        if ((rate > 0.1) || (!useSampling))
            rate = 1.0; //no use in sampling
        else System.out.println("used sampling with rate: " + rate);
        final Histogram1DLight tmpHist =
                this.dataSet.blockingSketch(new Hist1DLightSketch(bDec, this.colName, null, rate));
        return tmpHist.createCDF();
    }

    Histogram1D prepareHist(int width, int height, int barWidth, boolean useSampling) {
        int bucketNum = width / barWidth;
        BucketsDescriptionEqSize bDec  =
                new BucketsDescriptionEqSize(this.colStat.getMin(), this.colStat.getMax(), bucketNum);
        //approximately what is needed to have error smaller than a single pixel
        double sampleSize  =  2 * height * height * bucketNum;
        double rate = sampleSize / this.colStat.getSize();
        if ((rate > 0.1) || (!useSampling))
            rate = 1.0; //no use in sampling
        else System.out.println("used sampling with rate: " + rate);
        return this.dataSet.blockingSketch(new Hist1DSketch(bDec, this.colName, null, rate));
    }

    @Test
    public void HistE2E() {
        CDFTest cdftest = new CDFTest();
        System.out.println("Number of rows is: " + cdftest.bigSize);
        long startTime, endTime;
        startTime = System.nanoTime();
        Histogram1DLight cdf = cdftest.prepareCDF(1000, 1000, false);
        endTime = System.nanoTime();
        System.out.println("Running time of cdf: " + (double) (endTime - startTime)/1000000000);
        startTime = System.nanoTime();
        Histogram1D hist = cdftest.prepareHist(1000, 100, 10, true);
        endTime = System.nanoTime();
        System.out.println("Running time of hist: " + (double) (endTime - startTime)/1000000000);
        startTime = System.nanoTime();
        Histogram1D hist1 = cdftest.prepareHist(1000, 100, 10, false);
        endTime = System.nanoTime();
        System.out.println("Running time of hist1: " + (double) (endTime - startTime)/1000000000);
    }
}
