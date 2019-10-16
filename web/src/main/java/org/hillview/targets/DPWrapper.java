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
import org.hillview.maps.IdMap;
import org.hillview.sketches.PrecomputedSketch;
import org.hillview.sketches.results.BucketsInfo;
import org.hillview.sketches.results.DataRange;
import org.hillview.table.PrivacySchema;
import org.hillview.dataset.api.IJson;
import org.hillview.sketches.results.TableSummary;
import org.hillview.table.ColumnDescription;
import org.hillview.table.Schema;
import org.hillview.table.api.ITable;
import org.hillview.table.columns.ColumnQuantization;
import org.hillview.table.columns.StringColumnQuantization;
import org.hillview.table.filters.RangeFilterDescription;
import org.hillview.table.filters.RangeFilterPair;
import org.hillview.utils.Converters;
import org.hillview.utils.HillviewException;
import org.hillview.utils.JsonList;

import javax.annotation.Nullable;
import java.io.File;
import java.util.HashMap;
import java.util.function.BiFunction;

/**
 * This class offers support for differentially-private queries on a data source.
 */
public class DPWrapper {
    // For each column the range allowed after filtering
    private final HashMap<String, RangeFilterDescription> columnLimits;
    /* Global parameters for differentially-private histograms using the binary mechanism. */
    protected PrivacySchema privacySchema;

    DPWrapper(PrivacySchema privacySchema) {
        this.columnLimits = new HashMap<String, RangeFilterDescription>();
        this.privacySchema = privacySchema;
    }

    DPWrapper(DPWrapper other) {
        this.privacySchema = other.privacySchema;
        this.columnLimits = new HashMap<String, RangeFilterDescription>(other.columnLimits);
    }

    /**
     * If a dataset composed of files is private, we expect that a corresponding directory exists at the root server
     * with a matching name and with such a file inside.
     */
    private static final String PRIVACY_METADATA_NAME = "privacy_metadata.json";

    /**
     * If the privacy metadata file exists return the file name.
     * @param folder  Folder where we look for the file.
     * @return  null if the file does not exist, the file name otherwise.
     */
    @Nullable
    static String privacyMetadataFile(String folder) {
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
        RangeFilterDescription rf = this.columnLimits.get(filter.cd.name);
        ColumnQuantization q = this.privacySchema.quantization(filter.cd.name);
        filter = filter.intersect(Converters.checkNull(q));
        if (rf != null)
            filter = filter.intersect(rf);
        this.columnLimits.put(filter.cd.name, filter);
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
            quantization = ((StringColumnQuantization)quantization).restrict(filter.minString, filter.maxString);
            result = quantization.getQuantiles(qa.stringsToSample);
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

    public void filterRange(RpcRequest request, RpcRequestContext context,
                            IPrivateDataset target,
                            BiFunction<IPrivateDataset, HillviewComputation, IPrivateDataset> constructor) {
        RangeFilterDescription filter = request.parseArgs(RangeFilterDescription.class);
        if (filter.complement)
            throw new HillviewException("Only filters on contiguous range are supported");
        IdMap<ITable> map = new IdMap<ITable>();
        target.runMap(target.getDataset(), map, (e, c) -> {
            IPrivateDataset result = constructor.apply(target, c);
            result.getWrapper().filter(filter);
            return result;
        }, request, context);
    }

    public void filter2DRange(RpcRequest request, RpcRequestContext context,
                              IPrivateDataset target,
                              BiFunction<IPrivateDataset, HillviewComputation, IPrivateDataset> constructor) {
        RangeFilterPair filter = request.parseArgs(RangeFilterPair.class);
        if (filter.first.complement || filter.second.complement)
            throw new HillviewException("Only filters on contiguous range are supported");
        IdMap<ITable> map = new IdMap<ITable>();
        target.runMap(target.getDataset(), map, (e, c) -> {
            IPrivateDataset result = constructor.apply(target, c);
            result.getWrapper().filter(filter.first);
            result.getWrapper().filter(filter.second);
            return result;
        }, request, context);
    }
}
