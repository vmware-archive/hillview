/*
 * Copyright (c) 2019 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hillview.targets;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.hillview.*;
import org.hillview.sketches.results.TableSummary;
import org.hillview.utils.*;
import org.hillview.dataStructures.QuantilesArgs;
import org.hillview.security.PersistedKeyLoader;
import org.hillview.security.SecureLaplace;
import org.hillview.sketches.results.BucketsInfo;
import org.hillview.sketches.results.DataRange;
import org.hillview.sketches.results.StringQuantiles;
import org.hillview.storage.ColumnLimits;
import org.hillview.table.PrivacySchema;
import org.hillview.dataset.api.IJson;
import org.hillview.table.ColumnDescription;
import org.hillview.table.columns.ColumnQuantization;
import org.hillview.table.columns.StringColumnQuantization;
import org.hillview.table.filters.RangeFilterDescription;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * This class offers support for differentially-private queries on a data source.
 */
public class DPWrapper {
    // This class is introduced to allow multiple DPWrappers to share a single
    // privacy schema.  This enables the curator to change the schema once
    // for a whole set of related views.
    static class PrivacySchemaContainer {
        private PrivacySchema privacySchema;
        PrivacySchemaContainer(PrivacySchema ps) {
            this.privacySchema = ps;
        }

        /* Set the privacy schema locally but do not persist it. */
        void setPrivacySchema(PrivacySchema ps) {
            this.privacySchema = ps;
        }
    }

    // For each column the range allowed after filtering
    final ColumnLimits columnLimits;
    /* Global parameters for differentially-private histograms using the binary mechanism. */
    protected final PrivacySchemaContainer container;
    final String schemaFilename;

    public final SecureLaplace laplace;

    public DPWrapper(PrivacySchema privacySchema, String schemaFilename) {
        this.columnLimits = new ColumnLimits();
        this.container = new PrivacySchemaContainer(privacySchema);
        this.schemaFilename = schemaFilename;
        this.laplace = this.getOrCreateLaplace();
    }

    DPWrapper(DPWrapper other) {
        this.container = other.container;
        this.columnLimits = new ColumnLimits(other.columnLimits);
        this.schemaFilename = other.schemaFilename;
        this.laplace = this.getOrCreateLaplace();
    }

    /**
     * If a dataset composed of files is private, we expect that a corresponding directory exists at the root server
     * with a matching name and with such a file inside.
     */
    private static final String PRIVACY_METADATA_NAME = "privacy_metadata.json";

    private static final String PRIVACY_METADATA_DIRECTORY = "data/metadata/differential-privacy/";

    /**
     * The filename at which to look for the generated secret key for this table.
     */
    private static final String KEY_NAME = "hillview_root_key";

    /**
     * Column index for counts corresponding to the entire table.
     * TODO: Make sure this doesn't conflict with counts on "all columns".
     */
    public static final int TABLE_COLUMN_INDEX = -1;

    /**
     * If the privacy metadata file exists return the file name.
     * @param folder  Folder where we look for the file.
     * @return  null if the file does not exist, the file name otherwise.
     */
    @Nullable
    public static String privacyMetadataFile(String folder) {
        File metadataFile = new File(PRIVACY_METADATA_DIRECTORY + folder, PRIVACY_METADATA_NAME);
        if (metadataFile.getAbsoluteFile().exists())
            return metadataFile.toString();
        return null;
    }

    private SecureLaplace getOrCreateLaplace() {
        String basename = Utilities.getFolder(this.schemaFilename);
        Path keyFilePath = Paths.get(basename, DPWrapper.KEY_NAME);

        // Retrieves key stored on disk or creates a new key and persists it, if no such key exists.
        PersistedKeyLoader loader = new PersistedKeyLoader(keyFilePath);
        return new SecureLaplace(loader);
    }

    public PrivacySchema getPrivacySchema() {
        return this.container.privacySchema;
    }

    void setPrivacySchema(PrivacySchema ps) {
        this.container.setPrivacySchema(ps);
    }

    /* Persist the privacy schema to disk, overwriting the existing schema. */
    void savePrivacySchema() {
        try {
            this.container.privacySchema.saveToFile(this.schemaFilename);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static class TableMetadata extends TableTarget.TableMetadata {
        static final long serialVersionUID = 1;
        public final long rowCountConfidence;
        public final PrivacySchema privacyMetadata;

        public TableMetadata(TableTarget.TableMetadata meta,
                             PrivacySchema ps, long rowCountConfidence) {
            super(meta);
            this.privacyMetadata = ps;
            this.rowCountConfidence = rowCountConfidence;
        }

        @Override
        public JsonElement toJsonTree() {
            JsonObject result = (JsonObject)super.toJsonTree();
            result.addProperty("rowCountConfidence", this.rowCountConfidence);
            result.add("privacyMetadata", IJson.gsonInstance.toJsonTree(this.privacyMetadata));
            return result;
        }
    }

    TableMetadata addPrivateMetadata(TableTarget.TableMetadata metadata) {
        double epsilon = this.getPrivacySchema().epsilon();
        Noise noise = DPWrapper.computeCountNoise(TABLE_COLUMN_INDEX, SpecialBucket.TotalCount, epsilon, this.laplace);
        long rowCount = metadata.rowCount + Converters.toLong(noise.getNoise());
        @SuppressWarnings("ConstantConditions")
        long rowCountConfidence = Converters.toLong(PrivacyUtils.laplaceCI(
                1, 1.0/epsilon, PrivacyUtils.DEFAULT_ALPHA).second);
        return new TableMetadata(new TableTarget.TableMetadata(
                new TableSummary(metadata.schema, rowCount), metadata.geoMetadata),
                this.container.privacySchema, rowCountConfidence);
    }

    /**
     * This represents symbolically some buckets that are allocated
     * outside the normal quantization intervals.
     */
    public enum SpecialBucket {
        TotalCount, // A bucket representing the whole tree
        NullCount,  // A bucket just for the null values
        DistinctCount  // A bucket for the distinct count query
    }

    public static Noise computeCountNoise(Integer columnIndex, SpecialBucket bucket, double epsilon, SecureLaplace laplace) {
        // TODO(pratiksha): check that this is correct.
        if (epsilon <= 0)
            throw new RuntimeException("Zero epsilon");
        int index;
        switch (bucket) {
            case TotalCount:
                index = -1;
                break;
            case NullCount:
                index = -2;
                break;
            case DistinctCount:
                index = -3;
                break;
            default:
                throw new RuntimeException("Unexpected special bucket " + bucket);
        }
        double scale = 1 / epsilon;
        return new Noise(laplace.sampleLaplace(columnIndex, scale, new Pair<Integer, Integer>(index, index)), 2 * Math.pow(scale, 2));
    }

    public void filter(RangeFilterDescription filter) {
        RangeFilterDescription rf = this.columnLimits.get(filter.cd.name);
        ColumnQuantization q = this.getPrivacySchema().quantization(filter.cd.name);
        filter = filter.intersect(q);
        filter = filter.intersect(rf);
        this.columnLimits.put(filter);
    }

    @Nullable
    public RangeFilterDescription getRange(String column) {
        return this.columnLimits.get(column);
    }

    private BucketsInfo getRange(QuantilesArgs qa) {
        ColumnDescription cd = qa.cd;
        ColumnQuantization quantization = this.getPrivacySchema().quantization(cd.name);
        RangeFilterDescription filter = this.columnLimits.get(cd.name);
        BucketsInfo quantiles = Converters.checkNull(quantization).getQuantiles(qa.stringsToSample);
        if (filter == null)
            return quantiles;

        if (cd.kind.isString()) {
            StringColumnQuantization q = (StringColumnQuantization) quantization;
            String left = q.roundDown(filter.minString);
            String right = q.roundDown(filter.maxString);
            JsonList<String> included = new JsonList<String>();
            boolean before = true;
            for (int i = 0; i < q.leftBoundaries.length; i++) {
                if (before) {
                    if (left == null || q.leftBoundaries[i].compareTo(left) >= 0)
                        before = false;
                }
                if (!before)
                    included.add(q.leftBoundaries[i]);
                if (right == null || q.leftBoundaries[i].compareTo(right) >= 0)
                    break;
            }
            JsonList<String> sampled = new JsonList<String>();
            Utilities.equiSpaced(included, qa.stringsToSample, sampled);
            boolean allStringsKnown = sampled.size() == included.size();
            return new StringQuantiles(sampled, q.globalMax, allStringsKnown, -1, -1);
        } else {
            BucketsInfo result = new DataRange(quantization.roundDown(filter.min),
                    quantization.roundDown(filter.max));
            result.missingCount = -1;
            result.presentCount = -1;
            return result;
        }
    }

    void getDataQuantiles(RpcRequest request, RpcRequestContext context,
                          IPrivateDataset target) {
        QuantilesArgs[] args = request.parseArgs(QuantilesArgs[].class);
        JsonList<BucketsInfo> bi = Linq.mapToList(args, this::getRange);
        target.returnResult(new JsonList<BucketsInfo>(bi), request, context);
    }

    int getColumnIndex(String... colNames) {
        return this.container.privacySchema.getColumnIndex(colNames);
    }
}
