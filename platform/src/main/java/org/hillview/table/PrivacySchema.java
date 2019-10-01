package org.hillview.table;

import org.hillview.dataset.api.IJson;
import org.hillview.table.rows.PrivacyMetadata;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;

/**
 * PrivacySchema contains additional metadata for columns that are visualized using the binary mechanism for
 * differential privacy, as per Chan, Song, Shi, TISSEC '11 (https://eprint.iacr.org/2010/076.pdf).
 * Epsilon budgets can be specified for both single-column (1-d) queries as well as for multi-column queries.
 * Multi-column metadata are specified with the key
 * */
public class PrivacySchema implements IJson {
    HashMap<String, PrivacyMetadata> metadata;

    public PrivacySchema(HashMap<String, PrivacyMetadata> metadata) {
        this.metadata = metadata;
    }

    public PrivacyMetadata get(String colName) {
        return metadata.get(colName);
    }

    public PrivacyMetadata get(String[] colNames) {
        Arrays.sort(colNames);
        String key = "";
        for (int i = 0; i < colNames.length - 1; i++) {
            key += colNames[i];
            key += "+";
        }
        key += colNames[colNames.length - 1];
        return metadata.get(key);
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
