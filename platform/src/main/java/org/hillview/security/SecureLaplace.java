package org.hillview.security;


import org.hillview.dataset.api.Pair;
import org.hillview.utils.ByteUtil;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;
import java.security.*;

import static org.hillview.utils.ByteUtil.INT_SIZE;
import static org.hillview.utils.ByteUtil.byteArrayToLong;

public class SecureLaplace {
    private Key sk;
    private byte[] scratchBytes = new byte[4*INT_SIZE]; // For sampling Laplace noise on intervals.
    private Cipher aes;
    private double normalizer = Math.pow(2, -53);

    public SecureLaplace() {
        SecureRandom random = new SecureRandom();
        byte[] key = new byte[32];
        random.nextBytes(key);

        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch ( NoSuchAlgorithmException e ) {
            throw new RuntimeException("Could not find digest algorithm");
        }

        byte[] hash = digest.digest(key); // Just in case we got an adversarial input.
        this.sk = new SecretKeySpec(hash, "AES");

        try {
            this.aes = Cipher.getInstance("AES/CBC/PKCS5Padding");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (NoSuchPaddingException e) {
            e.printStackTrace();
        }

        try {
            aes.init(Cipher.ENCRYPT_MODE, this.sk);
        } catch (InvalidKeyException e) {
            e.printStackTrace();
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
        ByteUtil.intPairToByteArray(index, this.scratchBytes);

        byte[] bytes = new byte[0];
        try {
            bytes = this.aes.doFinal(scratchBytes);
        } catch (IllegalBlockSizeException e) {
            e.printStackTrace();
        } catch (BadPaddingException e) {
            e.printStackTrace();
        }

        long val = byteArrayToLong(bytes);
        double sampledValue = (double)val * this.normalizer;
        return sampledValue;
    }

    /**
     * Sample a value from Laplace(0, scale) using a pseudorandom function indexed by index.
     * Note that this implementation is vulnerable to the attack described in
     * "On Significance of the Least Significant Bits For Differential Privacy", Mironov, CCS 2012.
     */
    public double sampleLaplace(Pair<Integer, Integer> index, double scale) {
        double unif = this.sampleUniform(index);

        double r = 0.5 - unif;
        if ( r < 0 ) {
            return -1 * scale * Math.log(1 - 2*(-1 * r));
        } else {
            return scale * Math.log(1 - 2*r);
        }
    }


    /***** Equivalent functions in two dimensions *****/
    /* TODO (pratiksha): Check that AES is using the correct number of input bytes. */

    /**
     * Securely sample a random double uniformly in [0, 1). This implementation returns
     * a uniform value that is a multiple of 2^-53 using a pseudorandom function indexed
     * by index.
     *
     * NOTE: This implementation is *not* thread-safe. scratchBytes is reused without a lock.
     * */
    private double sampleUniform(Pair<Integer, Integer> index1, Pair<Integer, Integer> index2) {
        ByteUtil.intPairPairToByteArray(index1, index2, this.scratchBytes);

        byte[] bytes = new byte[0];
        try {
            bytes = this.aes.doFinal(scratchBytes);
        } catch (IllegalBlockSizeException e) {
            e.printStackTrace();
        } catch (BadPaddingException e) {
            e.printStackTrace();
        }

        long val = byteArrayToLong(bytes);
        double sampledValue = (double)val * this.normalizer;
        return sampledValue;
    }

    /**
     * Sample a value from Laplace(0, scale) using a pseudorandom function indexed by index.
     * Note that this implementation is vulnerable to the attack described in
     * "On Significance of the Least Significant Bits For Differential Privacy", Mironov, CCS 2012.
     */
    public double sampleLaplace(Pair<Integer, Integer> index1, Pair<Integer, Integer> index2, double scale) {
        double unif = this.sampleUniform(index1, index2);

        double r = 0.5 - unif;
        if ( r < 0 ) {
            return -1 * scale * Math.log(1 - 2*(-1 * r));
        } else {
            return scale * Math.log(1 - 2*r);
        }
    }

}
