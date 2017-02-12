package org.hiero.sketch;
import org.hiero.sketch.table.api.IStringConverter;

/**
 * A converter for integers. Created solely for testing. Will be scrapped once
 * decent converters are implemented.
 */
public class IntConverter implements IStringConverter{
    @Override
    public double asDouble(final String string) {
        return (double) Integer.parseInt(string);
    }
}
