package org.hillview.security;

import javax.crypto.spec.SecretKeySpec;
import java.security.Key;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

import static org.hillview.utils.Utilities.INT_SIZE;

/**
 * TestKeyLoader provides functionality to create new random keys for use in tests that average over randomness.
 */
public class TestKeyLoader implements KeyLoader {
    public byte[] keyBase;
    public int index;

    public TestKeyLoader() {
        SecureRandom random = new SecureRandom();
        byte[] key = new byte[32 - INT_SIZE];
        random.nextBytes(key);
        this.keyBase = key;
        this.index = 0;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public Key getOrCreateKey() {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }

        byte[] key = new byte[32];
        System.arraycopy(this.keyBase, 0, key, 0, this.keyBase.length);

        // Set the last 4 bytes to the index
        for (int i = INT_SIZE; i >= 0; i--) {
            key[31 - INT_SIZE + i] = (byte)(this.index  >> 8*i);
        }

        byte[] hash = digest.digest(key); // Just in case we got an adversarial input.
        return new SecretKeySpec(hash, "AES");
    }
}
