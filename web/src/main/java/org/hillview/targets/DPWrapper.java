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
import org.hillview.dataStructures.QuantilesArgs;
import org.hillview.dataset.api.ISketch;
import org.hillview.dataset.api.Pair;
import org.hillview.sketches.PrecomputedSketch;
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
import org.hillview.utils.Converters;
import org.hillview.utils.HillviewException;
import org.hillview.utils.JsonList;

import javax.annotation.Nullable;
import java.io.File;
import java.util.function.BiFunction;

/**
 * This class offers support for differentially-private queries on a data source.
 */
public class DPWrapper {
    // For each column the range allowed after filtering
    final ColumnLimits columnLimits;
    /* Global parameters for differentially-private histograms using the binary mechanism. */
    protected PrivacySchema privacySchema;

    public DPWrapper(PrivacySchema privacySchema) {
        this.columnLimits = new ColumnLimits();
        this.privacySchema = privacySchema;
    }

    public DPWrapper(DPWrapper other) {
        this.privacySchema = other.privacySchema;
        this.columnLimits = new ColumnLimits(other.columnLimits);
    }

    /**
     * If a dataset composed of files is private, we expect that a corresponding directory exists at the root server
     * with a matching name and with such a file inside.
     */
    public static final String PRIVACY_METADATA_NAME = "privacy_metadata.json";

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

    public static class PrivacySummary implements IJson {
        @Nullable
        public Schema schema;
        public long rowCount;
        @Nullable
        public PrivacySchema metadata;
    }

    PrivacySummary addPrivateMetadata(TableSummary summary) {
        PrivacySummary pSumm = new PrivacySummary();
        pSumm.schema = summary.schema;
        pSumm.metadata = this.privacySchema;
        // TODO(pratiksha): add noise to the row count too.
        pSumm.rowCount = summary.rowCount;
        return pSumm;
    }

    public void filter(RangeFilterDescription filter) {
        if (filter.complement)
            throw new HillviewException("Only filters on contiguous range are supported");
        RangeFilterDescription rf = this.columnLimits.get(filter.cd.name);
        ColumnQuantization q = this.privacySchema.quantization(filter.cd.name);
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
        ColumnQuantization quantization = this.privacySchema.quantization(cd.name);
        RangeFilterDescription filter = this.columnLimits.get(cd.name);
        BucketsInfo quantiles = Converters.checkNull(quantization).getQuantiles(qa.stringsToSample);
        if (filter == null)
            return quantiles;

        BucketsInfo result;
        if (cd.kind.isString()) {
            StringColumnQuantization q = (StringColumnQuantization) quantization;
            String left = q.roundDown(filter.minString);
            String right = q.roundDown(filter.maxString);
            JsonList<String> included = new JsonList<String>();
            boolean before = true;
            for (int i = 0; i < q.leftBoundaries.length; i++) {
                if (before) {
                    if (q.leftBoundaries[i].equals(left))
                        before = false;
                }
                if (!before)
                    included.add(q.leftBoundaries[i]);
                if (q.leftBoundaries[i].equals(right))
                    break;
            }
            return new StringQuantiles(included, q.globalMax, true, -1, -1);
        } else {
            result = new DataRange(quantization.roundDown(filter.min),
                    quantization.roundDown(filter.max));
        }
        result.missingCount = -1;
        result.presentCount = -1;
        return result;
    }

    // The following are methods used in common by various Private targets;
    // technically we should use multiple inheritance to inherit these methods
    // but in Java this is not possible, so we put them here.

    void getDataQuantiles1D(RpcRequest request, RpcRequestContext context,
                            IPrivateDataset target) {
        QuantilesArgs[] args = request.parseArgs(QuantilesArgs[].class);
        assert args.length == 1;
        BucketsInfo retRange = this.getRange(args[0]);
        ISketch<ITable, BucketsInfo> sk = new PrecomputedSketch<ITable, BucketsInfo>(retRange);
        target.runCompleteSketch(
                target.getDataset(), sk, (e, c) -> new JsonList<BucketsInfo>(e), request, context);
    }

    void getDataQuantiles2D(RpcRequest request, RpcRequestContext context,
                            IPrivateDataset target) {
        QuantilesArgs[] args = request.parseArgs(QuantilesArgs[].class);
        assert args.length == 2;
        BucketsInfo retRange0 = this.getRange(args[0]);
        BucketsInfo retRange1 = this.getRange(args[1]);
        ISketch<ITable, Pair<BucketsInfo, BucketsInfo>> sk =
                new PrecomputedSketch<ITable, Pair<BucketsInfo, BucketsInfo>>(
                        new Pair<BucketsInfo, BucketsInfo>(retRange0, retRange1));
        BiFunction<Pair<BucketsInfo, BucketsInfo>, HillviewComputation, JsonList<BucketsInfo>> post =
                (e, c) -> new JsonList<BucketsInfo>(e.first, e.second);
        target.runCompleteSketch(target.getDataset(), sk, post, request, context);
    }
}
