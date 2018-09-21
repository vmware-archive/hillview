package org.hillview.sketches;

import net.openhft.hashing.LongHashFunction;
import org.hillview.table.Schema;

import java.io.Serializable;

public class CountSketchDescription implements Serializable{
    public int buckets;
    public int trials;
    public LongHashFunction[] hashFunction;
    public Schema schema;

    public CountSketchDescription(int buckets, int trials, long seed, Schema schema) {
        this.buckets = buckets;
        this.trials = trials;
        LongHashFunction hash = LongHashFunction.xx(seed);
        this.hashFunction = new LongHashFunction[trials];
        for (int i = 0; i < trials; i++)
            this.hashFunction[i] = LongHashFunction.xx(hash.hashInt(i));
        this.schema = schema;
    }

}
