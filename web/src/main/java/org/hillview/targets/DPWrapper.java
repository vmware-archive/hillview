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

import org.hillview.*;
import org.hillview.utils.*;
import org.hillview.dataStructures.QuantilesArgs;
import org.hillview.dataset.api.ISketch;
import org.hillview.dataset.api.Pair;
import org.hillview.security.PersistedKeyLoader;
import org.hillview.security.SecureLaplace;
import org.hillview.dataset.PrecomputedSketch;
import org.hillview.sketches.results.BucketsInfo;
import org.hillview.sketches.results.DataRange;
import org.hillview.sketches.results.StringQuantiles;
import org.hillview.storage.ColumnLimits;
import org.hillview.table.PrivacySchema;
import org.hillview.dataset.api.IJson;
import org.hillview.sketches.results.TableSummary;
import org.hillview.table.ColumnDescription;
import org.hillview.table.Schema;
import org.hillview.table.api.ITable;
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
        File metadataFile = new File(folder, PRIVACY_METADATA_NAME);
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

    @SuppressWarnings({"nullable", "NotNullFieldNotInitialized"})
    public static class PrivacySummary implements IJson {
        static final long serialVersionUID = 1;

        @Nullable
        public Schema schema;
        public long rowCount;
        public long rowCountConfidence;
        public PrivacySchema metadata;
    }

    PrivacySummary addPrivateMetadata(TableSummary summary) {
        PrivacySummary pSumm = new PrivacySummary();
        pSumm.schema = summary.schema;
        pSumm.metadata = this.container.privacySchema;
        double epsilon = this.getPrivacySchema().epsilon();
        Noise noise = DPWrapper.computeCountNoise(TABLE_COLUMN_INDEX, SpecialBucket.TotalCount, epsilon, this.laplace);
        pSumm.rowCount = summary.rowCount + Converters.toLong(noise.getNoise());
        pSumm.rowCountConfidence = Converters.toLong(PrivacyUtils.laplaceCI(
                1, 1.0/epsilon, PrivacyUtils.DEFAULT_ALPHA).second);
        return pSumm;
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
        if (filter.complement)
            throw new HillviewException("Only filters on contiguous range are supported");
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

    // The following are methods used in common by various Private targets;
    // technically we should use multiple inheritance to inherit these methods
    // but in Java this is not possible, so we put them here.
    void getDataQuantiles1D(RpcRequest request, RpcRequestContext context,
                            IPrivateDataset target) {
        QuantilesArgs[] args = request.parseArgs(QuantilesArgs[].class);
        assert args.length == 1;
        BucketsInfo retRange = this.getRange(args[0]);
        ISketch<ITable, JsonList<BucketsInfo>> sk =
                new PrecomputedSketch<ITable, JsonList<BucketsInfo>>(
                        new JsonList<BucketsInfo>(retRange));
        target.runCompleteSketch(target.getDataset(), sk, request, context);
    }

    void getDataQuantiles2D(RpcRequest request, RpcRequestContext context,
                            IPrivateDataset target) {
        QuantilesArgs[] args = request.parseArgs(QuantilesArgs[].class);
        assert args.length == 2;
        BucketsInfo retRange0 = this.getRange(args[0]);
        BucketsInfo retRange1 = this.getRange(args[1]);
        ISketch<ITable, JsonList<BucketsInfo>> sk =
                new PrecomputedSketch<ITable, JsonList<BucketsInfo>>(
                        new JsonList<BucketsInfo>(retRange0, retRange1));
        target.runCompleteSketch(target.getDataset(), sk, request, context);
    }

    void getDataQuantiles3D(RpcRequest request, RpcRequestContext context,
                            IPrivateDataset target) {
        QuantilesArgs[] args = request.parseArgs(QuantilesArgs[].class);
        assert args.length == 3;
        BucketsInfo retRange0 = this.getRange(args[0]);
        BucketsInfo retRange1 = this.getRange(args[1]);
        BucketsInfo retRange2 = this.getRange(args[2]);
        ISketch<ITable, JsonList<BucketsInfo>> sk =
                new PrecomputedSketch<ITable, JsonList<BucketsInfo>>(
                        new JsonList<BucketsInfo>(retRange0, retRange1, retRange2));
        target.runCompleteSketch(target.getDataset(), sk, request, context);
    }

    int getColumnIndex(String... colNames) {
        return this.container.privacySchema.getColumnIndex(colNames);
    }
}
