package org.hillview.security;

import org.hillview.utils.HillviewLogger;

import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Key;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

public class PersistedKeyLoader implements KeyLoader {
    private final Path keyPath;

    public PersistedKeyLoader(Path keyFilePath) {
        this.keyPath = keyFilePath;
    }

    public Key getOrCreateKey() {
        if (Files.exists(this.keyPath)) {
            return this.loadKey(this.keyPath);
        } else {
            try {
                HillviewLogger.instance.info("No key found, generating new");
                SecureRandom random = new SecureRandom();
                byte[] key = new byte[32];
                random.nextBytes(key);
                MessageDigest digest = null;
                digest = MessageDigest.getInstance("SHA-256");
                byte[] hash = digest.digest(key); // Just in case we got an adversarial input.
                Key sk = new SecretKeySpec(hash, "AES");

                Files.write(this.keyPath, sk.getEncoded());

                return sk;
            } catch (NoSuchAlgorithmException | IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private Key loadKey(Path keyFilePath) {
        HillviewLogger.instance.info("Loading key...");
        byte[] bytes;
        try {
            bytes = Files.readAllBytes(keyFilePath);
            HillviewLogger.instance.info("success.");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new SecretKeySpec(bytes, "AES");
    }
}
