package org.hiero.sketch.table;

public class SemiExplicitConverter extends BaseExplicitConverter {
    final double defaultValue;
    public SemiExplicitConverter (double defValue) {
        super();
        this.defaultValue = defValue;
    }

    /* Will return a default value when string is not known */
    @Override
    public double asDouble(final String string) {
        if (this.stringValue.containsKey(string))
            return this.stringValue.get(string);
        else return this.defaultValue;
    }

}
