package org.hiero.sketches;

import org.hiero.table.api.IColumn;
import org.hiero.table.api.IMembershipSet;
import org.hiero.table.api.IRowIterator;
import org.hiero.utils.Converters;
import org.hiero.utils.HashUtil;

/**
 * A class that computes an approximation of the number of distinct elements in a column. Elements
 * are identified via their hashcode. The class uses the HyperLogLog algorithm for large estimates
 * and LinearCounting algorithm for small estimates.
 */
public class HLogLog {
    private final int regNum; //number of registers
    private final int logregNum;
    private final byte[] registers;
    private final int seed;

    /**
     * @param logRegNum the logarithm of the number of registers. should be in 4...16
     *        Going beyond 16 would make the data structure big for no good reason.
     *        Setting logRegNum in at 10-12 should give roughly 2% accuracy most of the time
     */
    public HLogLog(int logRegNum, int seed) {
        HLogLog.checkSpaceValid(logRegNum);
        this.regNum = 1 << logRegNum;
        this.registers = new byte[this.regNum];
        this.logregNum = logRegNum;
        this.seed = seed;
    }

    /**
     * adds the int 'value' to the data structure.
     * Uses the first bits to identify the register and then counts trailing zeros
     * @param itemHash already assumed to be a random hash of the item
     */
    private void add(int itemHash) {
        int index =  itemHash >>> (Integer.SIZE - this.logregNum);
        byte zeros = (byte) (Integer.numberOfTrailingZeros(itemHash) + 1);
        if (zeros > this.registers[index])
            this.registers[index] = zeros;
    }

    /**
     * Creates a Hyperloglog data structure from a column and membership set. Uses the hash code
     * of the objects in the column as identifier.
     */
    public void createHLL(IColumn column, IMembershipSet memSet) {
        final IRowIterator myIter = memSet.getIterator();
        int currRow = myIter.getNextRow();
        while (currRow >= 0) {
            if (!column.isMissing(currRow)) {
                int value = Converters.checkNull(column.getObject(currRow)).hashCode();
                this.add(HashUtil.murmurHash3(this.seed, value));
            }
            currRow = myIter.getNextRow();
        }
    }

    public HLogLog union(HLogLog otherHLL) {
        if ((otherHLL.regNum != this.regNum) || (otherHLL.seed != this.seed))
            throw new IllegalArgumentException("attempted union of non matching HLogLog classes");
        HLogLog result = new HLogLog(this.logregNum, this.seed);
        for (int i = 0; i < this.regNum; i++)
            result.registers[i] = (byte) Integer.max(this.registers[i], otherHLL.registers[i]);
        return result;
    }

    /**
     * @return an estimation of the number of distinct items
     */
    public long distinctItemsEstimator() {
        double alpha;
        switch (this.logregNum) { //set the parameters for the log log estimator
            case 4: alpha = 0.673;
                break;
            case 5: alpha = 0.697;
                break;
            case 6: alpha = 0.709;
                break;
            default: alpha = 0.7213 / (1 + (1.079 / this.regNum));
        }
        double rawEstimate = 0;
        int zeroRegs = 0;
        for (int i = 0; i < this.regNum; i++) {
            rawEstimate += Math.pow(2, -this.registers[i]);
            if (this.registers[i] == 0)
                zeroRegs ++;
        }
        rawEstimate = 1 / rawEstimate;
        rawEstimate = rawEstimate * alpha * this.regNum * this.regNum;
        long result = Math.round(rawEstimate);
        if ((zeroRegs > 0) && (rawEstimate < (2.5 * this.regNum)))
            result = Math.round(this.regNum * (Math.log(this.regNum / (double) zeroRegs)));
        else if (rawEstimate > (Integer.MAX_VALUE / 30 ))
            result = (long) (- Math.pow(2,32) * Math.log(1 - (rawEstimate /  Math.pow(2,32))));
        return result;

        /*if ((rawEstimate >= (2.5 * this.regNum)) || (zeroRegs == 0)) {
            result =  Math.round(rawEstimate);
        }
        else if (rawEstimate < (Integer.MAX_VALUE / 30 )) {
            return Math.round(this.regNum * (Math.log(this.regNum / (double) zeroRegs)));
        }
        else return (long) (- Math.pow(2,32) * Math.log(1 - (rawEstimate /  Math.pow(2,32))));*/
    }

    /**
     * @param logSpaceSize the logarithm of the number of registers in the HLL structure.
     * Going beyond 16 is too big and does not
     * add accuracy. Going below 4 does not save Space.
     */
    public static void checkSpaceValid(int logSpaceSize) {
        if ((logSpaceSize > 16) || (logSpaceSize < 4))
            throw new IllegalArgumentException("HLogLog initialized with logSpaceSize out of range");
    }
}
