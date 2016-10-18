package org.hiero.sketch;

import org.junit.Test;

/**
 * Created by parik on 10/18/16.
 */
public class ParikTest {
    @Test
    public void myTest1() {

        String num = "1332.4";
        String not_num = "This is not a number";
        System.out.println(Double.parseDouble(num));
        System.out.println(Double.parseDouble(not_num));
    }
}