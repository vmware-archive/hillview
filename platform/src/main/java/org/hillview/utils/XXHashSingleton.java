package org.hillview.utils;

import net.openhft.hashing.LongHashFunction;

public class XXHashSingleton {

    private XXHashSingleton(){
        myHash = LongHashFunction.xx();
    }
    private LongHashFunction myHash;
    public LongHashFunction getHash() { return myHash; }

    private static class XXHashSingletonHelper{
        private static final XXHashSingleton INSTANCE = new XXHashSingleton();
    }

    public static XXHashSingleton getInstance() {
        return XXHashSingletonHelper.INSTANCE;
    }
}
