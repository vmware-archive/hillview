package org.hillview.table.filters;

import org.apache.commons.lang.StringUtils;
import org.hillview.table.api.IStringFilter;

import javax.annotation.Nullable;
import java.util.regex.Pattern;

public class StringFilter implements IStringFilter {
    private final StringFilterDescription stringFilterDescription;
    boolean missing;  // if true we look for missing values;
    @Nullable
    String compareTo;
    @Nullable
    Pattern regEx;

    public StringFilter(StringFilterDescription stringFilterDescription) {
        org.junit.Assert.assertNotNull(stringFilterDescription);
        this.stringFilterDescription = stringFilterDescription;
        if (stringFilterDescription.compareValue == null)
            this.missing = true;
        else {
            this.missing = false;
            if (stringFilterDescription.asRegEx) {
                this.regEx = Pattern.compile(stringFilterDescription.compareValue,
                        stringFilterDescription.caseSensitive ? 0 : Pattern.CASE_INSENSITIVE);
            } else {
                this.compareTo = stringFilterDescription.caseSensitive ?
                        stringFilterDescription.compareValue :
                        stringFilterDescription.compareValue.toLowerCase();
            }
        }
    }

    public boolean test(@Nullable String curString) {
        boolean result;
        if (this.missing || (curString == null))    //Look for missing values
            result = (this.missing && (curString == null));
        else if (this.stringFilterDescription.asRegEx) //RegEx matching
            result = this.regEx.matcher(curString).matches();
        else if (this.stringFilterDescription.asSubString) // Substring matching
            result = this.stringFilterDescription.caseSensitive ?
                        curString.contains(this.compareTo):
                        StringUtils.containsIgnoreCase(curString, this.compareTo);
        else //Exact matching
            result = this.stringFilterDescription.caseSensitive ?
                    curString.equals(this.compareTo):
                    curString.equalsIgnoreCase(this.compareTo);
        return result ^ this.stringFilterDescription.complement;
    }
}
