package org.hillview.table;

import org.hillview.table.api.IStringConverter;
import org.hillview.table.api.IStringConverterDescription;

import javax.annotation.Nullable;

/**
 * A string converter that does nothing
 */
public class NoStringConverter implements IStringConverter, IStringConverterDescription {
    private static final NoStringConverter INSTANCE = new NoStringConverter();

    @Override
    public double asDouble(@Nullable final String string) {
        throw new UnsupportedOperationException("NoStringConverter.asDouble() invoked");
    }

    @Override
    public IStringConverter getConverter() {
        return INSTANCE;
    }

    public static IStringConverterDescription getDescriptionInstance() {
        return INSTANCE;
    }

    public static IStringConverter getConverterInstance() {
        return INSTANCE;
    }
}
