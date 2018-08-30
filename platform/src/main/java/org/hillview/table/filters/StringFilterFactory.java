package org.hillview.table.filters;

import org.apache.commons.lang.StringUtils;
import org.hillview.table.api.IStringFilter;

import javax.annotation.Nullable;
import java.util.regex.Pattern;

public class StringFilterFactory {

    public IStringFilter getFilter(StringFilterDescription stringFilterDescription) {
        assert(stringFilterDescription != null);
        if (stringFilterDescription.compareValue == null)
            return new MissingValuesFilter(stringFilterDescription);
        else {
            if (stringFilterDescription.asRegEx) {
                return new RegExFilter(stringFilterDescription);
            } else if (stringFilterDescription.asSubString) {
                return new SubStringFilter(stringFilterDescription);
            } else
                return new ExactCompFilter(stringFilterDescription);
        }
    }

    class MissingValuesFilter implements IStringFilter {
        private final StringFilterDescription stringFilterDescription;

        public MissingValuesFilter(StringFilterDescription stringFilterDescription) {
            this.stringFilterDescription = stringFilterDescription;
        }

        public boolean test(@Nullable String curString) {
            return (curString == null)^this.stringFilterDescription.complement;
        }
    }

    class RegExFilter implements IStringFilter {
        private final StringFilterDescription stringFilterDescription;
        private final Pattern regEx;

        public RegExFilter(StringFilterDescription stringFilterDescription){
            this.stringFilterDescription = stringFilterDescription;
            this.regEx = Pattern.compile(stringFilterDescription.compareValue,
                    stringFilterDescription.caseSensitive ? 0 : Pattern.CASE_INSENSITIVE);
        }

        public boolean test(@Nullable String curString) {
            boolean result = (curString != null) && this.regEx.matcher(curString).matches();
            return result^this.stringFilterDescription.complement;
        }
    }

    class SubStringFilter implements IStringFilter {
        private final StringFilterDescription stringFilterDescription;
        private final String compareTo;

        public SubStringFilter(StringFilterDescription stringFilterDescription) {
            this.stringFilterDescription = stringFilterDescription;
            this.compareTo = stringFilterDescription.caseSensitive ?
                    stringFilterDescription.compareValue :
                    stringFilterDescription.compareValue.toLowerCase();
        }

        public boolean test(@Nullable String curString) {
            boolean result = (curString != null) && (this.stringFilterDescription.caseSensitive ?
                    curString.contains(this.compareTo) :
                    StringUtils.containsIgnoreCase(curString, this.compareTo));
            return result^this.stringFilterDescription.complement;
        }
    }

    class ExactCompFilter implements IStringFilter {
        private final StringFilterDescription stringFilterDescription;
        private final String compareTo;

        public ExactCompFilter(StringFilterDescription stringFilterDescription) {
            this.stringFilterDescription = stringFilterDescription;
            this.compareTo = stringFilterDescription.caseSensitive ?
                    stringFilterDescription.compareValue :
                    stringFilterDescription.compareValue.toLowerCase();
        }

        public boolean test(@Nullable String curString) {
            boolean result = (curString != null) && (this.stringFilterDescription.caseSensitive ?
                    curString.equals(this.compareTo) :
                    curString.equalsIgnoreCase(this.compareTo));
            return result^this.stringFilterDescription.complement;
        }
    }
}
