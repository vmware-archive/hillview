/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * This file contains lots of classes for accessing the remote TableTarget.java class.
 */

// I can't use an enum for ContentsKind because JSON deserialization does not
// return an enum from a string.
import {RpcRequest, RemoteObject, CombineOperators, Renderer} from "./rpc";
import {FullPage, IDataView, Resolution} from "./ui";
import {EqualityFilterDescription} from "./equalityFilter";
import {ICancellable, PartialResult, Triple} from "./util";
import {PointSet2D} from "./lamp";
export type ContentsKind = "Category" | "Json" | "String" | "Integer" | "Double" | "Date" | "Interval";
export function asContentsKind(kind: string): ContentsKind {
    switch (kind) {
        case "Category": {
            return "Category";
        }
        case "Json": {
            return "Json";
        }
        case "String": {
            return "String";
        }
        case "Integer": {
            return "Integer";
        }
        case "Double": {
            return "Double";
        }
        case "Date": {
            return "Date";
        }
        case "Interval": {
            return "Interval";
        }
        default: {
            throw new TypeError(`String ${kind} is not a kind.`);
        }
    }
}

export function isNumeric(kind: ContentsKind): boolean {
    return kind == "Integer" || kind == "Double";
}

export interface IColumnDescription {
    readonly kind: ContentsKind;
    readonly name: string;
    readonly allowMissing: boolean;
}

// Direct counterpart to Java class
export class ColumnDescription implements IColumnDescription {
    readonly kind: ContentsKind;
    readonly name: string;
    readonly allowMissing: boolean;

    constructor(v : IColumnDescription) {
        this.kind = v.kind;
        this.name = v.name;
        this.allowMissing = v.allowMissing;
    }
}

// Direct counterpart to Java class
export interface Schema {
    [index: number] : IColumnDescription;
    length: number;
}

export interface RowView {
    count: number;
    values: any[];
}

// Direct counterpart to Java class
export interface ColumnSortOrientation {
    columnDescription: IColumnDescription;
    isAscending: boolean;
}

export interface IDistinctStrings {
    uniqueStrings: string[];
    // This may be true if there are too many distinct strings in a column.
    truncated: boolean;
    // Number of values in the column containing the strings.
    columnSize: number;
}

export class RangeInfo {
    constructor(public columnName: string,
                // The following is only used for categorical columns
                public allNames?: string[]) {}
}

// same as Java class
export interface Histogram {
    buckets: number[]
    missingData: number;
    outOfRange: number;
}

// same as Java class
export interface BasicColStats {
    momentCount: number;
    min: number;
    max: number;
    moments: Array<number>;
    presentCount: number;
    missingCount: number;
}

// Same as Java class
export interface ColumnAndRange {
    min: number;
    max: number;
    samplingRate: number;
    columnName: string;
    bucketCount: number;
    cdfBucketCount: number;
    bucketBoundaries: string[];
}

export interface FilterDescription {
    min: number;
    max: number;
    columnName: string;
    complement: boolean;
    bucketBoundaries: string[];
}

/**
 * This class methods that correspond directly to TableTarget.java methods.
 */
export class RemoteTableObject extends RemoteObject {
    constructor(remoteObjectId: string) {
        super(remoteObjectId);
    }

    public createRangeRequest(r: RangeInfo): RpcRequest {
        return this.createRpcRequest("range", r);
    }

    public createZipRequest(r: RemoteObject): RpcRequest {
        return this.createRpcRequest("zip", r.remoteObjectId);
    }

    public createQuantileRequest(rowCount: number, o: RecordOrder, position: number): RpcRequest {
        return this.createRpcRequest("quantile", {
            precision: 100,
            tableSize: rowCount,
            order: o,
            position: position
        });
    }

    public createNextKRequest(order: RecordOrder, firstRow: any[]): RpcRequest {
        let nextKArgs = {
            order: order,
            firstRow: firstRow,
            rowsOnScreen: Resolution.tableRowsOnScreen
        };
        return this.createRpcRequest("getNextK", nextKArgs);
    }

    public createGetSchemaRequest(): RpcRequest {
        return this.createRpcRequest("getSchema", null);
    }

    public createHLogLogRequest(colName: string) : RpcRequest {
    	return this.createRpcRequest("hLogLog", colName);
    }

    public createRange2DRequest(r1: RangeInfo, r2: RangeInfo): RpcRequest {
        return this.createRpcRequest("range2D", [r1, r2]);
    }

    public createRange2DColsRequest(c1: string, c2: string): RpcRequest {
        let r1: RangeInfo = { columnName: c1, allNames: null };
        let r2: RangeInfo = { columnName: c2, allNames: null };
        return this.createRange2DRequest(r1, r2);
    }

    public createHeavyHittersRequest(columns: IColumnDescription[], percent: number): RpcRequest {
        return this.createRpcRequest("heavyHitters", {columns: columns, amount: percent});
    }

    public createCheckHeavyRequest(r: RemoteObject, schema: Schema): RpcRequest {
        return this.createRpcRequest("checkHeavy", {
            hittersId: r.remoteObjectId,
            schema: schema
        });
    }

    public createFilterHeavyRequest(r: RemoteObject, schema: Schema): RpcRequest {
        return this.createRpcRequest("filterHeavy", {
            hittersId: r.remoteObjectId,
            schema: schema
        });
    }

    public createProjectToEigenVectorsRequest(r: RemoteObject, dimension: number): RpcRequest {
        return this.createRpcRequest("projectToEigenVectors", {
            id: r.remoteObjectId,
            numComponents: dimension
        });
    }

    public createFilterEqualityRequest(filter: EqualityFilterDescription): RpcRequest {
        return this.createRpcRequest("filterEquality", filter);
    }

    public createCorrelationMatrixRequest(columnNames: string[]): RpcRequest {
        return this.createRpcRequest("correlationMatrix", {columnNames: columnNames});
    }

    public createFilterRequest(f: FilterDescription): RpcRequest {
        return this.createRpcRequest("filterRange", f);
    }

    public createFilter2DRequest(xRange: FilterDescription, yRange: FilterDescription): RpcRequest {
        return this.createRpcRequest("filter2DRange", {first: xRange, second: yRange});
    }

    public createHeatMapRequest(x: ColumnAndRange, y: ColumnAndRange): RpcRequest {
        return this.createRpcRequest("heatMap", { first: x, second: y });
    }


    public createHeatMap3DRequest(data: Triple<ColumnAndRange, ColumnAndRange, ColumnAndRange>): RpcRequest {
        return this.createRpcRequest("heatMap3D", data);
    }

    public createHistogramRequest(info: ColumnAndRange): RpcRequest {
        return this.createRpcRequest("histogram", info);
    }

    public createSetOperationRequest(setOp: CombineOperators): RpcRequest {
        return this.createRpcRequest("setOperation", CombineOperators[setOp]);
    }

    public createSampledControlPointsRequest(rowCount: number, numSamples: number, columnNames: string[], seed: number = 1) {
        return this.createRpcRequest("sampledControlPoints", {rowCount: rowCount, numSamples: numSamples, columnNames: columnNames, seed: seed});
    }

    public createCategoricalCentroidsControlPointsRequest(categoricalColumnName: string, numericalColumnNames: string[]) {
        return this.createRpcRequest("categoricalCentroidsControlPoints", {categoricalColumnName: categoricalColumnName, numericalColumnNames: numericalColumnNames});
    }

    public createMDSProjectionRequest(id: string, seed: number = 1) {
        return this.createRpcRequest("makeMDSProjection", {id: id, seed: seed});
    }

    public createLAMPMapRequest(controlPointsId: string, colNames: string[], controlPoints: PointSet2D, newColNames: string[]) {
        return this.createRpcRequest("lampMap", {controlPointsId: controlPointsId, colNames: colNames, newLowDimControlPoints: controlPoints, newColNames: newColNames});
    }
}

export abstract class RemoteTableObjectView extends RemoteTableObject implements IDataView {
    protected topLevel: HTMLElement;

    constructor(remoteObjectId: string, protected page: FullPage) {
        super(remoteObjectId);
        this.setPage(page);
    }

    setPage(page: FullPage) {
        if (page == null)
            throw("null FullPage");
        this.page = page;
    }

    getPage() : FullPage {
        if (this.page == null)
            throw("Page not set");
        return this.page;
    }

    public abstract refresh(): void;

    public getHTMLRepresentation(): HTMLElement {
        return this.topLevel;
    }

    public scrollIntoView() {
        this.getHTMLRepresentation().scrollIntoView( { block: "end", behavior: "smooth" } );
    }
}

/**
 * All strings that can appear in a categorical column.
 */
export class DistinctStrings implements IDistinctStrings {
    public uniqueStrings: string[];
    // This may be true if there are too many distinct strings in a column.
    public truncated: boolean;
    // Number of values in the column containing the strings.
    public columnSize: number;

    public constructor(ds: IDistinctStrings) {
        this.uniqueStrings = ds.uniqueStrings;
        this.truncated = ds.truncated;
        this.columnSize = ds.columnSize;
        this.uniqueStrings.sort();
    }

    public size(): number { return this.uniqueStrings.length; }

    public getRangeInfo(colName: string): RangeInfo {
        return {
            columnName: colName,
            allNames: this.uniqueStrings
        };
    }

    /**
     * Returns all strings numbered between min and max.
     * @param min    Minimum string number
     * @param max    Maximum string number
     * @param bucketCount
     * @returns {string[]}
     */
    public categoriesInRange(min: number, max: number, bucketCount: number): string[] {
        let boundaries: string[] = null;
        if (min <= 0)
            min = 0;
        if (max >= this.uniqueStrings.length - 1)
            max = this.uniqueStrings.length - 1;
        max = Math.floor(max);
        min = Math.ceil(min);
        let range = max - min;
        if (range <= 0)
            bucketCount = 1;

        if (this.uniqueStrings != null) {
            if (bucketCount >= range) {
                boundaries = this.uniqueStrings.slice(min, max + 1);  // slice end is exclusive
            } else {
                boundaries = [];
                for (let i = 0; i <= bucketCount; i++) {
                    let index = min + Math.round(i * range / bucketCount);
                    boundaries.push(this.uniqueStrings[index]);
                }
            }
        }
        return boundaries;
    }

    public get(index: number): string {
        index = Math.round(<number>index);
        if (index >= 0 && index < this.uniqueStrings.length)
            return this.uniqueStrings[index];
        return null;
    }
}

// Direct counterpart to Java class
export class RecordOrder {
    constructor(public sortOrientationList: Array<ColumnSortOrientation>) {}
    public length(): number { return this.sortOrientationList.length; }
    public get(i: number): ColumnSortOrientation { return this.sortOrientationList[i]; }

    // Find the index of a specific column; return -1 if columns is not in the sort order
    public find(col: string): number {
        for (let i = 0; i < this.length(); i++)
            if (this.sortOrientationList[i].columnDescription.name == col)
                return i;
        return -1;
    }
    public hide(col: string): void {
        let index = this.find(col);
        if (index == -1)
        // already hidden
            return;
        this.sortOrientationList.splice(index, 1);
    }
    public sortFirst(cso: ColumnSortOrientation) {
        let index = this.find(cso.columnDescription.name);
        if (index != -1)
            this.sortOrientationList.splice(index, 1);
        this.sortOrientationList.splice(0, 0, cso);
    }
    public show(cso: ColumnSortOrientation) {
        let index = this.find(cso.columnDescription.name);
        if (index != -1)
            this.sortOrientationList.splice(index, 1);
        this.sortOrientationList.push(cso);
    }
    public showIfNotVisible(cso: ColumnSortOrientation) {
        let index = this.find(cso.columnDescription.name);
        if (index == -1)
            this.sortOrientationList.push(cso);
    }
    public clone(): RecordOrder {
        return new RecordOrder(this.sortOrientationList.slice(0));
    }
    // Returns a new object
    public invert(): RecordOrder {
        let result = new Array<ColumnSortOrientation>(this.sortOrientationList.length);
        for (let i in this.sortOrientationList) {
            let cso = this.sortOrientationList[i];
            result[i] = {
                isAscending: !cso.isAscending,
                columnDescription: cso.columnDescription
            };
        }
        return new RecordOrder(result);
    }

    protected static coToString(cso: ColumnSortOrientation): string {
        return cso.columnDescription.name + " " + (cso.isAscending ? "up" : "down");
    }
    public toString(): string {
        let result = "";
        for (let i = 0; i < this.sortOrientationList.length; i++)
            result += RecordOrder.coToString(this.sortOrientationList[i]);
        return result;
    }
}

/// A renderer that receives a remoteObjectId for a RemoteTableObject.
export abstract class RemoteTableRenderer extends Renderer<string> {
    protected remoteObject: RemoteTableObject;

    public constructor(public page: FullPage,
                       public operation: ICancellable,
                       public description: string) {
        super(page, operation, description);
        this.remoteObject = null;
    }

    public onNext(value: PartialResult<string>) {
        super.onNext(value);
        if (value.data != null) {
            if (this.remoteObject != null)
                throw "Remote object already set " + this.remoteObject;
            this.remoteObject = new RemoteTableObject(value.data);
        }
    }
}

// A zip receiver receives the result of a Zip operation on
// two IDataSet<ITable> objects (an IDataSet<Pair<ITable, ITable>>,
// and applies to the pair the specified set operation setOp.
export class ZipReceiver extends RemoteTableRenderer {
    public constructor(page: FullPage,
                       operation: ICancellable,
                       protected setOp: CombineOperators,
                       // receiver constructs the renderer that is used to display
                       // the result after combining
                       protected receiver: (page: FullPage, operation: ICancellable) => RemoteTableRenderer) {
        super(page, operation, "zip");
    }

    onCompleted(): void {
        super.finished();
        if (this.remoteObject == null)
            return;

        let rr = this.remoteObject.createSetOperationRequest(this.setOp);
        let rec = this.receiver(this.page, rr);
        rr.invoke(rec);
    }
}
