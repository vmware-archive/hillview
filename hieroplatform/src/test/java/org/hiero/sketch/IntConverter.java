package org.hiero.sketch;

import org.hiero.sketch.table.api.IStringConverter;

/**
 * Created by uwieder on 2/12/17.
 */
public class IntConverter implements IStringConverter{
    @Override
    public double asDouble(String string) {
        return (double) Integer.parseInt(string);
    }
}
