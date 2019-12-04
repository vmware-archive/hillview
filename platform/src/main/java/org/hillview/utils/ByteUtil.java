package org.hillview.utils;

import org.hillview.dataset.api.Pair;

import java.util.Arrays;

public class ByteUtil {
    public static final int INT_SIZE = 4;

    /* assumes arr has 2*INT_SIZE space allocated */
    public static void intPairToByteArray(Pair<Integer, Integer> p, /*out*/byte[] arr) {
        Arrays.fill(arr, (byte)0);

        if ( arr.length < 2 ) {
            throw new RuntimeException("Not enough bytes allocated for output");
        }

        for ( int i = INT_SIZE - 1; i >= 0; i-- ) {
            arr[i]          = (byte)(p.first  >> 8*i);
            arr[i+INT_SIZE] = (byte)(p.second >> 8*i);
        }
    }

    public static void intPairPairToByteArray(Pair<Integer, Integer> p1, Pair<Integer, Integer> p2, /*out*/ byte[] arr) {
        Arrays.fill(arr, (byte)0);

        if ( arr.length < 2 ) {
            throw new RuntimeException("Not enough bytes allocated for output");
        }

        for ( int i = INT_SIZE - 1; i >= 0; i-- ) {
            arr[i]          = (byte)(p1.first  >> 8*i);
            arr[i+INT_SIZE] = (byte)(p1.second >> 8*i);
            arr[i+2*INT_SIZE] = (byte)(p2.first  >> 8*i);
            arr[i+3*INT_SIZE] = (byte)(p2.second >> 8*i);
        }
    }

    public static long byteArrayToLong(byte[] bytes) {
        if (bytes.length < 8) {
            throw new RuntimeException("Not enough bytes to convert to int");
        }

        long value = 0L;
        for (int i = 0; i < 7; i++)
        {
            value += ((long) bytes[i] & 0xffL) << (8 * i);
        }
        value += (((long) bytes[7] & 0xffL) << 48);
        value &=  ((1L << 53L) - 1L);

        return value;
    }
}
