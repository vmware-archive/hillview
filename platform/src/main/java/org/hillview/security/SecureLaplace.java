package org.hillview.security;


import org.hillview.dataset.api.Pair;
import org.hillview.utils.Utilities;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;
import java.security.*;

import static org.hillview.utils.Utilities.INT_SIZE;
import static org.hillview.utils.Utilities.byteArrayToLong;

public class SecureLaplace {
    private byte[] scratchBytes = new byte[4*INT_SIZE]; // For sampling Laplace noise on intervals.
    private Cipher aes;
    private double normalizer = Math.pow(2, -53);

    public SecureLaplace() {
        try {
            SecureRandom random = new SecureRandom();
            byte[] key = new byte[32];
            random.nextBytes(key);
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(key); // Just in case we got an adversarial input.
            Key sk = new SecretKeySpec(hash, "AES");
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
     *
     * NOTE: This implementation is *not* thread-safe. scratchBytes is reused without a lock.
     * */
    private double sampleUniform(Pair<Integer, Integer> index) {
        try {
            Utilities.intPairToByteArray(index, this.scratchBytes);
            byte[] bytes = this.aes.doFinal(scratchBytes);
            long val = byteArrayToLong(bytes);
            return (double)val * this.normalizer;
        } catch (IllegalBlockSizeException | BadPaddingException e) {
            throw new RuntimeException(e);
        }
    }

    private double rescale(double scale, double unif) {
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
    public double sampleLaplace(Pair<Integer, Integer> index, double scale) {
        double unif = this.sampleUniform(index);
        return rescale(scale, unif);
    }


    /* **** Equivalent functions in two dimensions *****/
    /* TODO (pratiksha): Check that AES is using the correct number of input bytes. */

    /**
     * Securely sample a random double uniformly in [0, 1). This implementation returns
     * a uniform value that is a multiple of 2^-53 using a pseudorandom function indexed
     * by index.
     *
     * NOTE: This implementation is *not* thread-safe. scratchBytes is reused without a lock.
     * */
    private double sampleUniform(Pair<Integer, Integer> index1, Pair<Integer, Integer> index2) {
        Utilities.intPairPairToByteArray(index1, index2, this.scratchBytes);
        try {
            byte[] bytes = this.aes.doFinal(scratchBytes);
            long val = byteArrayToLong(bytes);
            return (double)val * this.normalizer;
        } catch (IllegalBlockSizeException | BadPaddingException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Sample a value from Laplace(0, scale) using a pseudorandom function indexed by index.
     * Note that this implementation is vulnerable to the attack described in
     * "On Significance of the Least Significant Bits For Differential Privacy", Mironov, CCS 2012.
     */
    public double sampleLaplace(Pair<Integer, Integer> index1, Pair<Integer, Integer> index2, double scale) {
        double unif = this.sampleUniform(index1, index2);
        return rescale(scale, unif);
    }
}
