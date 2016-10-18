package org.hiero.sketch;

import org.hiero.sketch.table.ColumnDescription;
import org.hiero.sketch.table.IntArrayColumn;
import org.hiero.sketch.table.api.ContentsKind;
import org.junit.Test;

import java.util.BitSet;

import static junit.framework.TestCase.assertEquals;

/**
 * Created by parik on 10/17/16.
 */
public class IntArrayTest {

    /* Test for constructor using length, no missing avlues*/
    @Test
    public void testIntArrayOne() {
        IntArrayColumn col;
        final int size = 100;

        ColumnDescription desc = new ColumnDescription("test", ContentsKind.Int, true);
        col = new IntArrayColumn(desc, size);
        for (int i=0; i < size; i++)
            col.set(i, i);

        assertEquals( col.sizeInRows(), size );
        assertEquals( col.getInt(0), 0 );
        for (int i=0; i < size; i++) {
            assertEquals(i, col.getInt(i));
            assertEquals(false, col.isMissing(i));
        }
    }


    /* Test for constructor using length, allows missing avlues*/
    @Test
    public void testIntArrayTwo() {
        IntArrayColumn col;
        final int size = 10;

        ColumnDescription desc = new ColumnDescription("test", ContentsKind.Int, true);
        col = new IntArrayColumn(desc, size);
        for (int i=0; i < size; i++) {
            col.set(i, i);
            if (i % 3 == 0)
                col.setMissing(i);
        }


        //assertEquals( col.sizeInRows(), size );
        //assertEquals( col.getInt(0), 0 );
        for (int i=0; i < size; i++)
            if(col.isMissing(i))
               System.out.println("Missing");
            else
                System.out.println(col.getInt(i));
    }

    /* Test for constructor using two arrays for data and missing values*/
    @Test
    public void testIntArrayThree() {
        IntArrayColumn col;
        final int size = 50;
        int[] data;
        BitSet missing;

        ColumnDescription desc = new ColumnDescription("test", ContentsKind.Int, true);



        data = new int[size];
        missing = new BitSet(size);


        for (int i=0; i < size; i++) {
            data[i] = i * i;
            if (i % 5 == 0)
                missing.set(i);
        }


        col = new IntArrayColumn(desc, data, missing);
        //assertEquals( col.sizeInRows(), size );
        //assertEquals( col.getInt(0), 0 );
        for (int i=0; i < size; i++)
            if(col.isMissing(i))
                System.out.println("Missing");
            else
                System.out.println(col.getInt(i));
    }
}
