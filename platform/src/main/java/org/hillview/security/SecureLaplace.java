package org.hillview.security;


import org.hillview.utils.Pair;
import org.hillview.utils.Utilities;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import java.security.*;

import static org.hillview.utils.Utilities.INT_SIZE;
import static org.hillview.utils.Utilities.byteArrayToLong;

public class SecureLaplace {
    /**
     * For sampling Laplace noise on intervals.
     * For a query on a column with index I and a rectangle <x1, y1, x2, y2>,
     * scratchBytes is filled with [I, x1, y1, x2, y2].
     */
    private Cipher aes;

    private static final double NORMALIZER = Math.pow(2, -53);

    public SecureLaplace(KeyLoader keyLoader) {
        try {
            Key sk = keyLoader.getOrCreateKey();
            this.aes = Cipher.getInstance("AES/CBC/PKCS5Padding");
            this.aes.init(Cipher.ENCRYPT_MODE, sk);
        } catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Securely sample a random double uniformly in [0, 1). This implementation returns
     * a uniform value that is a multiple of 2^-53 using a pseudorandom function indexed
     * by index.
     */
    @SafeVarargs
    private final double sampleUniform(Integer columnIndex, Pair<Integer, Integer>... index) {
        byte[] scratchBytes = new byte[5*INT_SIZE];
        if (scratchBytes.length < index.length*INT_SIZE + 1) {
            throw new RuntimeException("Not enough bytes allocated to sample with " + index.length + " columns");
        }

        Utilities.intToByteArray(columnIndex, scratchBytes, 0);
        for (int i = 0; i < index.length; i++) {
            Utilities.intPairToByteArray(index[i], scratchBytes, INT_SIZE*(2*i + 1));
        }

        try {
            byte[] bytes = this.aes.doFinal(scratchBytes);
            long val = byteArrayToLong(bytes);
            return (double)val * NORMALIZER;
        } catch (IllegalBlockSizeException | BadPaddingException e) {
            throw new RuntimeException(e);
        }
    }

    private double uniformToLaplace(double scale, double unif) {
        double r = 0.5 - unif;
        if ( r < 0 ) {
            return -1 * scale * Math.log(1 - 2*(-1 * r));
        } else {
            return scale * Math.log(1 - 2*r);
        }
    }

    /**
     * Sample a value from Laplace(0, scale) using a pseudorandom function indexed by index.
     * Note that this implementation is vulnerable to the attack described in
     * "On Significance of the Least Significant Bits For Differential Privacy", Mironov, CCS 2012.
     */
    @SafeVarargs
    public final double sampleLaplace(Integer columnIndex, double scale, Pair<Integer, Integer>... index) {
        double unif = this.sampleUniform(columnIndex, index);
        return uniformToLaplace(scale, unif);
    }
}
