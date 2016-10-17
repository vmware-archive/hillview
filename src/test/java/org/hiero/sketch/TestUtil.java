package org.hiero.sketch;

import java.util.Arrays;

/**
 * Created by uwieder on 10/15/16.
 */
public class TestUtil {
    public void TestUtil(){}

    static public void Percentiles(long[] R1){
        Arrays.sort(R1);
        System.out.println("Percentiles: 0,10,20,50,90,99 ");
        System.out.println(R1[0]+ " , " + R1[R1.length/10]+ " , " + R1[R1.length/5]+ " , " +  R1[R1.length/2] + " , " + R1[9*R1.length/10] + " , " + R1[R1.length-1]);

    }
}
