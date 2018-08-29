package org.hillview.table.filters;

import javax.annotation.Nullable;
import java.io.Serializable;

public class StringFilterDescription implements Serializable {
    @Nullable
    public final String compareValue;
    public final boolean asSubString;
    public final boolean asRegEx;
    public final boolean caseSensitive;
    public final boolean complement;

    public StringFilterDescription(
            @Nullable String compareValue,  boolean asSubString, boolean asRegEx,
            boolean caseSensitive, boolean complement) {
        this.compareValue = compareValue;
        this.asSubString = asSubString;
        this.asRegEx = asRegEx;
        this.caseSensitive = caseSensitive;
        this.complement = complement;
    }

    public StringFilterDescription(@Nullable String compareValue,  boolean asSubString, boolean asRegEx,
            boolean caseSensitive) {
        this(compareValue, asSubString, asRegEx, caseSensitive, false);
    }

    public StringFilterDescription(@Nullable String compareValue) {
        this(compareValue, false, false, false, false);
    }
}

