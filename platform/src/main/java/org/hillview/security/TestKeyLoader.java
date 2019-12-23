package org.hillview.security;

import org.hillview.utils.HillviewLogger;

import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.nio.file.Files;
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
        MessageDigest digest = null;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }

        byte[] key = new byte[32];
        for (int i = 0; i < this.keyBase.length; i++) {
            key[i] = this.keyBase[i];
        }

        // Set the last 4 bytes to the index
        for (int i = INT_SIZE; i >= 0; i--) {
            key[31 - INT_SIZE + i] = (byte)(this.index  >> 8*i);
        }

        byte[] hash = digest.digest(key); // Just in case we got an adversarial input.
        Key sk = new SecretKeySpec(hash, "AES");
        return sk;
    }
}
