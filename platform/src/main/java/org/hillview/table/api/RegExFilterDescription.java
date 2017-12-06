package org.hillview.table.api;

import javax.annotation.Nullable;
import java.util.regex.Pattern;

public class RegExFilterDescription implements ITableFilterDescription {
    public final String column;
    @Nullable
    public final String regEx;

    public RegExFilterDescription(String column, String regEx) {
        this.column = column;
        this.regEx= regEx;
    }

    @Override
    public ITableFilter getFilter(ITable table) {
        return new RegExFilter(table);
    }

    public class RegExFilter implements ITableFilter {
        private final ColumnAndConverter column;
        private Pattern regEx;

        public RegExFilter(ITable table) {
            this.regEx = Pattern.compile(RegExFilterDescription.this.regEx);
            this.column = table.getLoadedColumn(RegExFilterDescription.this.column);
        }

        @Override
        public boolean test(int rowIndex) {
            return this.regEx.matcher(this.column.asString(rowIndex)).matches();
        }
    }
}
