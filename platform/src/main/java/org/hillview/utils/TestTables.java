/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hillview.utils;

import org.hillview.dataset.LocalDataSet;
import org.hillview.dataset.ParallelDataSet;
import org.hillview.dataset.api.IDataSet;
import org.hillview.dataset.api.Pair;
import org.hillview.table.ColumnDescription;
import org.hillview.table.SmallTable;
import org.hillview.table.Table;
import org.hillview.table.api.*;
import org.hillview.table.columns.*;
import org.hillview.table.membership.FullMembershipSet;
import org.hillview.table.membership.SparseMembershipSet;
import org.hillview.table.rows.RowSnapshot;
import org.jblas.DoubleMatrix;

import java.util.*;

/**
 * This class generates some constant tables for testing purposes.
 */
public class TestTables {
    /**
     * Can be used for testing.
     * @return A small table with some interesting contents.
     */
    public static Table testTable() {
        ColumnDescription c0 = new ColumnDescription("Name", ContentsKind.Category);
        ColumnDescription c1 = new ColumnDescription("Age", ContentsKind.Integer);
        CategoryArrayColumn sac = new CategoryArrayColumn(c0,
                new String[] { "Mike", "John", "Tom", "Bill", "Bill", "Smith", "Donald", "Bruce",
                               "Bob", "Frank", "Richard", "Steve", "Dave" });
        IntArrayColumn iac = new IntArrayColumn(c1, new int[] { 20, 30, 10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 });
        return new Table(Arrays.asList(sac, iac), null, null);
    }

    /**
     * Same as testTable, but with list columns.
     */
    public static Table testListTable() {
        Table t = testTable();
        CategoryListColumn sac = new CategoryListColumn(t.getLoadedColumn("Name").getDescription());
        IntListColumn iac = new IntListColumn(t.getLoadedColumn("Age").getDescription());
        IRowIterator row = t.getMembershipSet().getIterator();
        for (int r = row.getNextRow(); r >= 0; r = row.getNextRow()) {
            RowSnapshot rs = new RowSnapshot(t, r, t.getSchema());
            sac.append(rs.asString("Name"));
            iac.append(rs.getInt("Age"));
        }
        return new Table(Arrays.asList(sac, iac), null, null);
    }

    /**
     * Can be used for testing.
     * @return A small table with some repeated content.
     */
    public static Table testRepTable() {
        ColumnDescription c0 = new ColumnDescription("Name", ContentsKind.Category);
        ColumnDescription c1 = new ColumnDescription("Age", ContentsKind.Integer);
        CategoryArrayColumn sac = new CategoryArrayColumn(c0,
                new String[] { "Mike", "John", "Tom", "Bill", "Bill", "Smith", "Donald", "Bruce",
                        "Bob", "Frank", "Richard", "Steve", "Dave", "Mike", "Ed" });
        IntArrayColumn iac = new IntArrayColumn(c1, new int[] { 20, 30, 10, 10, 20, 30, 20, 30, 10,
                40, 40, 20, 10, 50, 60 });
        return new Table(Arrays.asList(sac, iac), null, null);
    }

    /**
     * Can be used for testing large tables with strings.
     * @param size Number of rows in the table
     * @param others Array of options in the "Name" column
     * @param count Number of occurrences of the 'test' string.
     * @param test The string that should occur 'count' times.
     * @return A table with an arbitrary number of rows. It contains 'count' rows that have 'test' in the Name column.
     */
    public static Table testLargeStringTable(int size, String[] others, int count, String test) {
        ColumnDescription c0 = new ColumnDescription("Name", ContentsKind.Category);
        ColumnDescription c1 = new ColumnDescription("Age", ContentsKind.Integer);

        //Assert.assertTrue(!Arrays.asList(others).contains(test));
        Random random = new Random(0);
        ArrayList<String> names = new ArrayList<String>();
        ArrayList<Integer> ages = new ArrayList<Integer>();
        for (int i = 0; i < size - count; i++) {
            String name = others[random.nextInt(others.length)];
            names.add(name);
            ages.add(random.nextInt(60) + 20);
        }

        // Add 'count' test names.
        for (int i = 0; i < count; i++) {
            names.add(test);
            ages.add(random.nextInt(60) + 20);
        }

        // Shuffle the lists, just to be sure.
        long seed = System.nanoTime();
        Collections.shuffle(names, new Random(seed));
        Collections.shuffle(ages, new Random(seed));

        StringArrayColumn sac = new StringArrayColumn(c0, names.toArray(new String[0]));
        IntArrayColumn iac = new IntArrayColumn(c1, ages.stream().mapToInt(i -> i).toArray());

        return new Table(Arrays.asList(sac, iac), null, null);
    }

    /**
     * Helper method that generates a List of random alphanumeric strings.
     * @param suppSize Number of strings
     * @param length Size of each string.
     */
    public static List<String> randStringList(int suppSize, int length) {
        String aToZ = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        Random random = new Random(1276449);
        ArrayList<String> names = new ArrayList<String>();
        for (int i = 0; i < suppSize; i++) {
            StringBuilder nextName = new StringBuilder();
            for (int j = 0; j < length; j++) {
                int index = random.nextInt(aToZ.length());
                nextName.append(aToZ.charAt(index));
            }
            names.add(nextName.toString());
        }
        return names;
    }

    /**
     * Generates a table containing a single column of (repeated) strings.
     * @param num the number of rows in the table
     * @param randString the set of strings that occur in the table.Can be viewed as the support of
     *                   the distribution.
     * @return A table containing a single column. Each row of this column is drawn randomly from
     * randString.
     */
    public static Pair<Table, SortedMap<String, Integer> > randStringTable(int num, List<String> randString) {
        int suppSize = randString.size();
        ArrayList<String> names = new ArrayList<String>();
        Random random = new Random(112358);
        int [] count = new int[suppSize];
        for (int i = 0; i< num; i++) {
            int index = random.nextInt(suppSize);
            names.add(randString.get(index));
            count[index] += 1;
        }
        SortedMap<String, Integer> dist= new TreeMap<>();
        for (int i = 0 ; i < suppSize; i++)
            if (count[i] > 0)
                dist.put(randString.get(i), count[i]);
        ColumnDescription c0 = new ColumnDescription("Name", ContentsKind.String);
        StringArrayColumn sac = new StringArrayColumn(c0, names.toArray(new String[0]));
        Table randStringTable = new Table(Arrays.asList(sac), null, null);
        return new Pair<Table, SortedMap<String, Integer>>(randStringTable, dist);
    }

    /**
     * Given a small list of strings (inpList) and a large list (allStrings), it returns the ranks
     * of the elements of the small list in the large list. Used for testing.
     */
    public static List<Integer> getRanks(List<String> inpList, List<String> allStrings) {
        Collections.sort(allStrings);
        SortedMap<String, Integer> sortedStrings= new TreeMap<>();
        for (int i = 0; i < allStrings.size(); i++)
            sortedStrings.put(allStrings.get(i), i);
        List<Integer> ranks = new ArrayList<>();
        for (String anInpList : inpList) {
            if (sortedStrings.get(anInpList) != null)
                ranks.add(sortedStrings.get(anInpList));
        }
        return ranks;
    }

    /**
     * @return A table which contains number from (1,..., range), where i occurs i^exp times.
     */
    public static SmallTable getPowerIntTable(final int range, double exp) {
        final List<IColumn> columns = new ArrayList<IColumn>(1);
        columns.add(IntArrayGenerator.getPowerIntArray(range, exp));
        return new SmallTable(columns);
    }

    /**
     * @return A table which contains number from (1,..., range), where i occurs i^2 times.
     */
    public static SmallTable getPowerIntTable(final int range) {
        return getPowerIntTable(range, 2);
    }

    public static SmallTable getZipfTable(final int range, double exp) {
        final List<IColumn> columns = new ArrayList<IColumn>(1);
        columns.add(IntArrayGenerator.getZipfArray(range, exp));
        return new SmallTable(columns);
    }


    /**
         * A table of integers whose rows are typically distinct. Each row is sampled randomly from a
         * domain of size 5^numCols*size. When numCols is small, some collisions are to be expected, but
         * generally the elements are distinct (each row in the range has a probability of 5^{-numCols}
         * of being sampled.)
         * @param size The size of the desired table
         * @param numCols The number of columns
         * @return A table of random integers.
         */
    public static SmallTable getIntTable(final int size, final int numCols) {
        final double exp = 1.0/numCols;
        final int range =  5*((int)Math.pow(size, exp));
        return getIntTable(size, numCols, range);
    }

    public static SmallTable getIntTable(final int size, final int numCols, int range) {
        Randomness rn = new Randomness(2); // we want deterministic random numbers for testing
        final List<IColumn> columns = new ArrayList<IColumn>(numCols);
        for (int i = 0; i < numCols; i++) {
            final String colName = "Column" + String.valueOf(i);
            columns.add(IntArrayGenerator.getRandIntArray(size, range, colName, rn));
        }
        return new SmallTable(columns);
    }


    /**
     * A table of integers with some missing values. Each column is the just the identity, but with
     * every multiple of some integer mod in {1,..,100} missing.
     * @param size The size of the desired table
     * @param numCols The number of columns
     * @return A table of integers with missing values.
     */
    public static SmallTable getMissingIntTable(final int size, final int numCols) {
        Randomness rn = new Randomness(2); // we want deterministic random numbers for testing
        final List<IColumn> columns = new ArrayList<IColumn>(numCols);
        for (int i = 0; i < numCols; i++) {
            int mod = rn.nextInt(99) + 1;
            final String colName = String.valueOf(i) + "Missing" + String.valueOf(mod);
            columns.add(IntArrayGenerator.getMissingIntArray(colName, size, mod));
        }
        return new SmallTable(columns);
    }
    /**
     * A table of integers where each row typically occurs multiple times. Each row is sampled
     * randomly from a domain of size size^{4/5}.  Collisions are to be expected, each tuple from
     * the range appears with frequency size^{1/5} in expectation.
     * @param size The size of the desired table
     * @param numCols The number of columns
     * @return A table of integers with repeated rows.
     */
    public static Table getRepIntTable(final int size, final int numCols) {
        Randomness rn = new Randomness(1); // we want deterministic random numbers for testing
        final List<IColumn> columns = new ArrayList<IColumn>(numCols);
        double exp = 0.8 / numCols;
        final int range =  ((int)Math.pow(size, exp));
        for (int i = 0; i < numCols; i++) {
            final String colName = "Column" + String.valueOf(i);
            columns.add(IntArrayGenerator.getRandIntArray(size, range, colName, rn));
        }
        final FullMembershipSet full = new FullMembershipSet(size);
        return new Table(columns, full, null, null);
    }

    /**
     * Method generates a table with a specified number of integer columns, where each column is
     * generated by the GetHeavyIntArray Method so the frequencies are geometrically increasing
     * @param numCols number of columns
     * @param size rows per column
     * @param base base parameter for GetHeavyIntArray
     * @param range range parameter for GetHeavyIntArray
     * @return A table of integers.
     */
    public static SmallTable getHeavyIntTable(final int numCols, final int size, final double base,
                                              final int range) {
        Randomness rn = new Randomness(3);
        final List<IColumn> columns = new ArrayList<IColumn>(numCols);
        for (int i = 0; i < numCols; i++) {
            final String colName = "Column" + String.valueOf(i);
            columns.add(IntArrayGenerator.getHeavyIntArray(size, base, range, colName, rn));
        }
        return new SmallTable(columns);
    }

    /**
     * Generates a table with a specified number of correlated columns. Each row has the same
     * absolute value in every column, they only differ in the sign (which is drawn randomly).
     * - Column 0 contains non-negative integers drawn at random from (0, range).
     * - The signs in the i^th column are  controlled by a parameter rho[i] in (0,1) which is
     * drawn at random. The sign of the i^th column is +1 with probability rho[i] and -1 with
     * probability (1 - rho[i]) independently for every row.
     * - The normalized correlation between Column 0 and Column i is 2*rho[i] - 1 in [-1,1], in
     * expectation.
     * @param size The number of rows.
     * @param numCols The number of columns.
     * @param range Each entry lies in {0, ..., range} in absolute value.
     * @return A table with correlated integer columns.
     */
    public static SmallTable getCorrelatedCols(final int size, final int numCols, final int range) {
        Randomness rn = new Randomness(100); // predictable randomness for testing
        double[] rho = new double[numCols];
        ColumnDescription[] desc = new ColumnDescription[numCols];
        String[] name = new String[numCols];
        List<IntArrayColumn> intCol = new ArrayList<IntArrayColumn>(numCols);
        for (int i =0; i<  numCols; i++) {
            name[i] = "Col" + String.valueOf(i);
            desc[i] = new ColumnDescription(name[i], ContentsKind.Integer);
            intCol.add(new IntArrayColumn(desc[i], size));
            rho[i] = ((i==0) ? 1 : (rho[i-1]*0.8));
            //System.out.printf("Rho %d = %f\n",i, rho[i]);
        }
        for (int i = 0; i < size; i++) {
            int k = rn.nextInt(range);
            for (int j = 0; j < numCols; j++) {
                double x = rn.nextDouble();
                intCol.get(j).set(i, ((x > rho[j]) ? -k: k));
            }
        }
        return new SmallTable(intCol);
    }

    /**
     * @param size Number of rows in the table.
     * @param numCols Number of columns in the table. Has to be >= 2.
     * @return A table where the 2nd column is a linear function of the 1st (with some noise). The rest of the columns
     * contains just small noise from a Gaussian distribution.
     */
    public static ITable getLinearTable(final int size, final int numCols) {
        Random rnd = new Random(42);
        double noise = 0.01;
        double a = 1.2;

        DoubleMatrix mat = new DoubleMatrix(size, numCols);
        for (int i = 0; i < size; i++) {
            // Make the first two columns linearly correlated.
            double x = rnd.nextDouble();
            mat.put(i, 0, x);
            double y = a * x + rnd.nextGaussian() * noise;
            mat.put(i, 1, y);

            // Fill the rest of the columns with noise.
            for (int j = 2; j < numCols; j++) {
                double z = noise * rnd.nextGaussian();
                mat.put(i, j, z);
            }
        }

        return BlasConversions.toTable(mat);
    }

    public static ITable getCentroidTestTable() {
        DoubleArrayColumn colX = new DoubleArrayColumn(
                new ColumnDescription("x", ContentsKind.Double),
                new double[]{1, 2, 2, 3, 4, 5, 5, 6}
        );
        DoubleArrayColumn colY = new DoubleArrayColumn(
                new ColumnDescription("y", ContentsKind.Double),
                new double[]{11, 10, 12, 11, 26, 25, 27, 26}
        );
        CategoryArrayColumn fruitType = new CategoryArrayColumn(
                new ColumnDescription("FruitType", ContentsKind.Category),
                new String[]{"Banana", "Banana", "Banana", "Banana", "Orange", "Orange", "Orange", "Orange"}
        );
        return new Table(Arrays.asList(colX, colY, fruitType), null, null);
    }

    /**
     * Return a numerical table with numBlobs clusters, every cluster containing numPointsPerBlob n-dimensional
     * points sampled from a Gaussian centered at a random point in the nD unit hyperbox, with a standard deviation
     * of stdDev.
     */
    public static ITable getNdGaussianBlobs(int numBlobs, int numPointsPerBlob, int n, double stdDev) {
        DoubleMatrix data = new DoubleMatrix(numBlobs * numPointsPerBlob, n);
        Random rnd = new Random(42);
        for (int i = 0; i < numBlobs; i++) {
            DoubleMatrix center = new DoubleMatrix(1, n);
            for (int j = 0; j < n; j++) {
                center.put(j, 20 * rnd.nextDouble());
            }
            for (int j = 0; j < numPointsPerBlob; j++) {
                DoubleMatrix delta = new DoubleMatrix(1, n);
                for (int k = 0; k < n; k++) {
                    delta.put(k, rnd.nextGaussian() * stdDev);
                }
                data.putRow(i * numPointsPerBlob + j, center.add(delta));
            }
        }
        return BlasConversions.toTable(data);
    }

    /**
     * Splits a Big Table into a list of Small Tables.
     * @param bigTable The big table
     * @param fragmentSize The size of each small Table
     * @return A list of small tables of size at most fragment size.
     */
    public static List<ITable> splitTable(ITable bigTable, int fragmentSize) {
        int tableSize = bigTable.getNumOfRows();
        int numTables = (tableSize / fragmentSize) + 1;
        List<ITable> tableList = new ArrayList<ITable>(numTables);
        int start = 0;
        while (start < tableSize) {
            int thisFragSize = Math.min(fragmentSize, tableSize - start);
            IMembershipSet members = new SparseMembershipSet(start, thisFragSize, tableSize);
            tableList.add(bigTable.selectRowsFromFullTable(members));
            start += fragmentSize;
        }
        return tableList;
    }

    /**
     * Creates a ParallelDataSet from a Big Table
     * @param bigTable The big table
     * @param fragmentSize The size of each small Table
     * @return A Parallel Data Set containing the data in the Big Table.
     */
    public static ParallelDataSet<ITable> makeParallel(ITable bigTable, int fragmentSize) {
        final List<ITable> tabList = splitTable(bigTable, fragmentSize);
        final ArrayList<IDataSet<ITable>> a = new ArrayList<IDataSet<ITable>>();
        for (ITable t : tabList) {
            LocalDataSet<ITable> ds = new LocalDataSet<ITable>(t);
            a.add(ds);
        }
        return new ParallelDataSet<ITable>(a);
    }
}
