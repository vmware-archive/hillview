package org.hiero.sketch.table;

import org.hiero.sketch.table.api.ContentsKind;
import org.hiero.utils.Randomness;

import java.security.InvalidParameterException;

public class GetIntArray {

    public static final ColumnDescription desc = new
            ColumnDescription("Identity", ContentsKind.Integer, true);

    public static IntArrayColumn generateIntArray(final int size) {
        final IntArrayColumn col = new IntArrayColumn(desc, size);
        for (int i = 0; i < size; i++) {
            col.set(i, i);
            if ((i % 5) == 0)
                col.setMissing(i);
        }
        return col;
    }

    public static IntArrayColumn getRandIntArray(final int size, final int range, final String name) {
        final ColumnDescription desc = new ColumnDescription(name, ContentsKind.Integer, false);
        final IntArrayColumn col = new IntArrayColumn(desc, size);
        final Randomness rn = Randomness.getInstance();
        for (int i = 0; i < size; i++)
            col.set(i, rn.nextInt(range));
        return col;
    }

    /**
     * Returns a column with a specified number of integers in the range
     * (1,..range), with the frequency of i proportional to base^i.
     * @param size the number of elements in the array.
     * @param base the base for the probabilities above.
     * @param range integers in the array lie in the interval (1,range)
     * @param name name of the column
     * @return An IntArray Column as described above.
     */
    public static IntArrayColumn getHeavyIntArray(final int size, final double base,
                                                  final int range, final String name) {
        if(base <= 1)
            throw new InvalidParameterException("Base should be  greater than 1.");
        final ColumnDescription desc = new ColumnDescription(name, ContentsKind.Integer, false);
        final IntArrayColumn col = new IntArrayColumn(desc, size);
        final Randomness rn = Randomness.getInstance();
        final int max = (int) Math.round(Math.pow(base,range));
        for (int i = 0; i < size; i++) {
            int j = rn.nextInt(max);
            int k = 0;
            while(j >= Math.pow(base,k))
                k++;
            col.set(i,k);
        }
        return col;
    }
}
