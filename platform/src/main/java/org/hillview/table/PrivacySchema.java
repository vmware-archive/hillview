package org.hillview.table;

import org.hillview.dataset.api.IJson;
import org.hillview.table.rows.PrivacyMetadata;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;

/* PrivacySchema contains additional metadata for computing private histograms on numerical columns. */
public class PrivacySchema implements IJson {
    HashMap<String, PrivacyMetadata> metadata;

    public PrivacySchema(HashMap<String, PrivacyMetadata> metadata) {
        this.metadata = metadata;
    }

    public PrivacyMetadata get(String colName) {
        return metadata.get(colName);
    }

    public static PrivacySchema loadFromString(String jsonString) {
        return IJson.gsonInstance.fromJson(jsonString, PrivacySchema.class);
    }

    /* One metadata object per column. */
    public static PrivacySchema loadFromFile(Path metadataFname) {
        String contents = "";
        try {
            byte[] encoded = Files.readAllBytes(metadataFname);
            contents = new String(encoded, StandardCharsets.US_ASCII);
        } catch ( java.io.IOException e ) {
            throw new RuntimeException(e);
        }

        return loadFromString(contents);
    }
}
