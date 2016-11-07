package org.hiero.sketch;

import org.hiero.sketch.table.ExplicitStringConverter;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

public class StringConverterTest {
    private final ExplicitStringConverter converter;

    public StringConverterTest() {
        this.converter = new ExplicitStringConverter();
        this.converter.set("S", 0);
    }

    @Test(expected=NullPointerException.class)
    public void getNonExistent() {
        this.converter.asDouble("T");
    }

    @Test
    public void testMember() {
        assertEquals( this.converter.asDouble("S"), 0.0 );
    }
}
