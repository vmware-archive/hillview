package org.hillview.test.table;

import org.hillview.table.PrivacySchema;
import org.hillview.table.rows.PrivacyMetadata;
import org.hillview.test.BaseTest;

import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;

public class PrivacySchemaTest extends BaseTest {
   @Test
    public void serializeMetadataTest() {
        HashMap<String, PrivacyMetadata> mdMap = new HashMap<String, PrivacyMetadata>();
        PrivacyMetadata md1 = new PrivacyMetadata(0.1, 10.5, 0.0, 123.45);
        PrivacyMetadata md2 = new PrivacyMetadata(0.5, 15.0, -0.5, 13.1);
        mdMap.put("col1", md1);
        mdMap.put("col2", md2);
        PrivacySchema mdSchema = new PrivacySchema(mdMap);
        String mdJson = mdSchema.toJson();
        String expected = "{\"metadata\":{\"col2\":{\"epsilon\":0.5,\"granularity\":15.0,\"globalMin\":-0.5,\"globalMax\":13.1}" +
                ",\"col1\":{\"epsilon\":0.1,\"granularity\":10.5,\"globalMin\":0.0,\"globalMax\":123.45}}}";
        assertEquals(expected, mdJson);
    }

    @Test
    public void deserializeMetadataTest() {
        String metadata = "{'metadata':{" +
                "'col1':{" +
                "'epsilon':0.1," +
                "'granularity':10.5," +
                "'globalMin':0.0," +
                "'globalMax':123.45}," +
                "'col2':{" +
                "'epsilon':0.5," +
                "'granularity':15.0," +
                "'globalMin':-0.5," +
                "'globalMax':13.1}" +
                "}}";
        PrivacySchema mdSchema = PrivacySchema.loadFromString(metadata);
        assertEquals(mdSchema.get("col1").epsilon, 0.1, 0.001);
    }

    @Test
    public void serializeMultipleColumnsTest() {
        HashMap<String, PrivacyMetadata> mdMap = new HashMap<>();
        PrivacyMetadata md1 = new PrivacyMetadata(0.1, 10.5, 0.0, 123.45);
        PrivacyMetadata md2 = new PrivacyMetadata(0.5, 15.0, -0.5, 13.1);
        PrivacyMetadata md12 = new PrivacyMetadata(0.25);
        mdMap.put("col1", md1);
        mdMap.put("col2", md2);
        mdMap.put("col1+col2", md12);
        PrivacySchema mdSchema = new PrivacySchema(mdMap);
        String mdJson = mdSchema.toJson();
        String expected = "{\"metadata\":{\"col1+col2\":{\"epsilon\":0.25,\"granularity\":0.0,\"globalMin\":0.0,\"globalMax\":0.0}," +
                "\"col2\":{\"epsilon\":0.5,\"granularity\":15.0,\"globalMin\":-0.5,\"globalMax\":13.1}," +
                "\"col1\":{\"epsilon\":0.1,\"granularity\":10.5,\"globalMin\":0.0,\"globalMax\":123.45}}}";
        assertEquals(expected, mdJson);
    }

    @Test
    public void deserializeMultipleColumnsTest() {
        String md = "{\"metadata\":{\"col1+col2\":{\"epsilon\":0.25}," +
                "\"col2\":{\"epsilon\":0.5,\"granularity\":15.0,\"globalMin\":-0.5,\"globalMax\":13.1}," +
                "\"col1\":{\"epsilon\":0.1,\"granularity\":10.5,\"globalMin\":0.0,\"globalMax\":123.45}}}";

        PrivacySchema mdSchema = PrivacySchema.loadFromString(md);
        assertEquals(mdSchema.get(new String[] {"col1", "col2"}).epsilon, 0.25, 0.001);
    }
}
