package org.hillview.table;

public class SemiExplicitConverter extends BaseExplicitConverter {
    final double defaultValue;
    public SemiExplicitConverter () {
        super();
        this.defaultValue = -1;
    }

    /* Will return a default value when string is not known */
    @Override
    public double asDouble(final String string) {
        if (this.stringValue.containsKey(string))
            return this.stringValue.get(string);
        else return this.defaultValue;
    }

}
