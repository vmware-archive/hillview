package org.hillview.table.rows;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenCustomHashMap;
import it.unimi.dsi.fastutil.ints.IntHash;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.hillview.table.Schema;
import org.hillview.table.api.ITable;
import org.hillview.utils.MutableInteger;

public class VirtualRowHashStrategy implements IntHash.Strategy {
    private final Schema schema;
    private final ITable data;
    private final VirtualRowSnapshot vrs;
    private final VirtualRowSnapshot vrs1;


    public VirtualRowHashStrategy(ITable data, Schema schema) {
        this.data = data;
        this.schema = schema;
        this.vrs1 = new VirtualRowSnapshot(data, schema);
        this.vrs = new VirtualRowSnapshot(data, schema);
    }

    public VirtualRowHashStrategy(ITable data) {
        this(data, data.getSchema());
    }

    @Override
    public int hashCode(int index) {
        this.vrs.setRow(index);
        return this.vrs.computeHashCode(schema);
    }

    @Override
    public boolean equals(int index, int otherIndex) {
        this.vrs.setRow(index);
        this.vrs1.setRow(otherIndex);
        return this.vrs.compareForEquality(this.vrs1, this.schema);
    }

    public Object2IntOpenHashMap<RowSnapshot> materializeHashMap(Int2ObjectOpenCustomHashMap<MutableInteger> hMap) {
        Object2IntOpenHashMap<RowSnapshot> hm = new Object2IntOpenHashMap<RowSnapshot>(hMap.size());
        for (ObjectIterator<Int2ObjectMap.Entry<MutableInteger>> it = hMap.int2ObjectEntrySet().fastIterator();
             it.hasNext(); ) {
            final Int2ObjectMap.Entry<MutableInteger> entry = it.next();
            hm.put(new RowSnapshot(this.data, entry.getIntKey(), this.schema), entry.getValue().get());
        }
        return hm;
    }
}
