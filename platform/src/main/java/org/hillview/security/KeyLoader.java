package org.hillview.security;

import java.security.Key;

public interface KeyLoader {
    Key getOrCreateKey();
}
