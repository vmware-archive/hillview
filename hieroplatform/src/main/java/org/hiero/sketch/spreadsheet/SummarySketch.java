package org.hiero.sketch.spreadsheet;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import org.hiero.sketch.dataset.api.IJson;
import org.hiero.sketch.dataset.api.ISketch;
import org.hiero.sketch.table.Schema;
import org.hiero.sketch.table.api.ITable;

import javax.annotation.Nullable;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A sketch which retrieves the Schema and size of a distributed table.
 * Two schemas can be added only if they are identical.
 * We use a null to represent a "zero" for the schemas.
 * (This Sketch is logically a ConcurrentSketch combining
 * an OptionMonoid[Schema] sketch and integer addition).
 */
public class SummarySketch implements ISketch<ITable, SummarySketch.TableSummary> {
    private static final Logger logger =
            Logger.getLogger(SummarySketch.class.getName());

    public static class TableSummary implements IJson {
        // The sketch zero() element can be produced without looking at the data at all.
        // So we need a way to represent a "zero" schema.  An empty schema is in principle
        // legal for a table, so we use a null to represent a yet "unknown" schema.
        @Nullable
        public final Schema schema;
        public final long   rowCount;

        public TableSummary(@Nullable Schema schema, long rowCount) {
            this.schema = schema;
            this.rowCount = rowCount;
        }

        public TableSummary() {
            this.schema = null;
            this.rowCount = 0;
        }

        public TableSummary add(TableSummary other) {
            Schema s = this.schema;
            if (this.schema == null)
                s = other.schema;
            else if (other.schema == null)
                s = this.schema;
            else if (!this.schema.equals(other.schema))
                throw new RuntimeException("Schemas differ");
            return new TableSummary(s, this.rowCount + other.rowCount);
        }

        @Override
        public JsonElement toJsonTree() {
            JsonObject result = new JsonObject();
            result.addProperty("rowCount", this.rowCount);
            if (this.schema == null)
                result.add("schema", JsonNull.INSTANCE);
            else
                result.add("schema", this.schema.toJsonTree());
            return result;
        }
    }

    @Override
    public TableSummary zero() {
        return new TableSummary();
    }

    @Override
    public TableSummary add(TableSummary left, TableSummary right) {
        return left.add(right);
    }

    @Override
    public TableSummary create(ITable data) {
        /*
         Testing code
        try {
            Thread.sleep(1000 * Randomness.getInstance().nextInt(5));
        } catch (InterruptedException unused) {}
        */
        logger.log(Level.INFO, "Completed sketch");
        return new TableSummary(data.getSchema(), data.getNumOfRows());
    }
}
