package org.hiero.utils;

import java.util.Random;

/**
 * A set of integers.
 * A simplified version of IntOpenHash from fastutil http://fastutil.di.unimi.it
 */
public class IntSet {
    private int[] key; /* The array of the linear probing */
    private int mask;
    private int n;  /* the size of the array - 1 */
    private boolean containsZero = false;  /* zero is reserved to signify an empty cell */
    private int size;

    private int maxFill;
    private final float f; /* the maximal load of the array */

    public IntSet(final int expected, final float f) {
        if ((f > 0.0F) && (f <= 1.0F)) {
            if (expected < 0) {
                throw new IllegalArgumentException("The expected number of elements must be " +
                        "non-negative");
            } else {
                this.f = f;
                this.n = HashUtil.arraySize(expected, f); /* size of array is power of two */
                this.mask = this.n - 1;
                this.maxFill = HashUtil.maxFill(this.n, f);
                this.key = new int[this.n + 1];
            }
        } else {
            throw new IllegalArgumentException("Load factor must be greater than 0 and " +
                    "smaller than or equal to 1");
        }
    }

    public IntSet(final int expected) {
        this(expected, 0.75F);
    }

    public IntSet() {
        this(16, 0.75F);
    }

    private int realSize() {
        return this.containsZero ? (this.size - 1) : this.size;
    }

    /**
     * @param k integer to add to the set
     * @return true if the set changed, false if the item is already in the set
     */
    @SuppressWarnings("UnusedReturnValue")
    public boolean add(final int k) {
        if (k == 0) {
            if (this.containsZero) {
                return false;
            }
            this.containsZero = true;
        } else {
            final int[] key = this.key;
            int pos;
            int curr;
            if ((curr = key[pos = HashUtil.murmurHash3(k) & this.mask]) != 0) {
                if (curr == k) {
                    return false;
                }
                while ((curr = key[(pos = (pos + 1) & this.mask)]) != 0) {
                    if (curr == k) {
                        return false;
                    }
                }
            }
            key[pos] = k;
        }
        if (this.size++ >= this.maxFill) {
            this.rehash(HashUtil.arraySize(this.size + 1, this.f));
        }
        return true;
    }

    public boolean contains(final int k) {
        if (k == 0) {
            return this.containsZero;
        } else {
            final int[] key = this.key;
            int curr;
            int pos;
            if((curr = key[pos = HashUtil.murmurHash3(k) & this.mask]) == 0) {
                return false;
            } else if(k == curr) {
                return true;
            } else {
                while((curr = key[(pos = (pos + 1) & this.mask)]) != 0) {
                    if(k == curr) {
                        return true;
                    }
                }
                return false;
            }
        }
    }

    public int size() {
        return this.size;
    }

    public boolean isEmpty() {
        return this.size == 0;
    }

    private void rehash(final int newN) {
        final int[] key = this.key;
        final int mask = newN - 1;
        final int[] newKey = new int[newN + 1];
        int i = this.n;
        int pos;
        for(int j = this.realSize(); j-- != 0; newKey[pos] = key[i]) {
            do {
                --i;
            } while(key[i] == 0);

            if (newKey[pos = HashUtil.murmurHash3(key[i]) & mask] != 0) {
                while (newKey[(pos = (pos + 1) & mask)] != 0) {}
            }
        }
        this.n = newN;
        this.mask = mask;
        this.maxFill = HashUtil.maxFill(this.n, this.f);
        this.key = newKey;
    }

    /**
     * @return a deep copy of IntSet
     */
    public IntSet copy() {
        final IntSet newSet = new IntSet(1, this.f);
        newSet.n = this.n;
        newSet.mask = this.mask;
        newSet.maxFill = this.maxFill;
        newSet.size = this.size;
        newSet.containsZero = this.containsZero;
        newSet.key = new int[this.n + 1];
        System.arraycopy(this.key, 0, newSet.key, 0, this.key.length);
        return newSet;
    }

    public IntSet sample(final int k, final long seed, final boolean useSeed) {
        if (k >= this.size)
            return this.copy();
        final IntSet sampleSet = new IntSet(k);
        final Random psg;
        int sampleSize;
        if (useSeed)
            psg = new Random(seed);
        else
            psg = new Random();
        int randomKey = psg.nextInt(this.n);
        if ((this.containsZero) && (randomKey == 0)) {  //sampling zero is done separately
            sampleSet.add(0);
            sampleSize = k-1;
        }
        else sampleSize = k;
        randomKey = psg.nextInt();
        for (int samples = 0; samples < sampleSize; samples++) {
            while (this.key[randomKey & this.mask] == 0)
                randomKey++;
            sampleSet.add(this.key[randomKey & this.mask]);
            randomKey++;
        }
        return sampleSet;
    }

    public IntSetIterator getIterator() {
        return new IntSetIterator();
    }

    /* Iterator for IntSet. Returns -1 when done. Assumes IntSet is not mutated */
    public class IntSetIterator {
        private int pos;
        private int c;
        private boolean mustReturnZero;

        private IntSetIterator() {
            this.pos = IntSet.this.n;
            this.c = IntSet.this.size;
            this.mustReturnZero = IntSet.this.containsZero;
        }

        public boolean hasNext() {
            return this.c != 0;
        }

        public int getNext() {
            if (!this.hasNext())
                return -1;
            --this.c;
            if (this.mustReturnZero) {
                this.mustReturnZero = false;
                return 0;
            }
            while (--this.pos >= 0) {
                if(IntSet.this.key[this.pos] != 0)
                    return IntSet.this.key[this.pos];
            }
            return -1;
        }
    }
}

