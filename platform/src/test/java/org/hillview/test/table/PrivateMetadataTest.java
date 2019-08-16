package org.hillview.test.table;

import org.hillview.table.PrivacySchema;
import org.hillview.table.rows.PrivacyMetadata;
import org.hillview.test.BaseTest;

import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;

public class PrivateMetadataTest extends BaseTest {
    @Test
    public void parseMetadataTest() {
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
        PrivacySchema mdSchema= PrivacySchema.loadFromString(metadata);
        assertEquals(mdSchema.get("col1").epsilon, 0.1, 0.001);
    }

    @Test
    public void serializeMetadataTest() {
        HashMap<String, PrivacyMetadata> mdMap = new HashMap<String, PrivacyMetadata>();
        PrivacyMetadata md1 = new PrivacyMetadata(0.1, 10.5, 0.0, 123.45);
        PrivacyMetadata md2 = new PrivacyMetadata(0.5, 15.0, -0.5, 13.1);
        mdMap.put("col1", md1);
        mdMap.put("col2", md2);
        PrivacySchema mdSchema = new PrivacySchema(mdMap);
        String mdJson = mdSchema.toJson();
        System.out.println(mdJson);
    }
}
