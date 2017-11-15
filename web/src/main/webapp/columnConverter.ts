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

import {Dialog} from "./ui/dialog";
import {allContentsKind, ContentsKind} from "./tableData";
import {TableView, RemoteTableReceiver} from "./table";
import {OnCompleteRenderer} from "./rpc";
import {ICancellable} from "./util";
import {FullPage} from "./ui/fullPage";

/**
 * A dialog to find out information about how to perform the conversion of the data in a column.
 */
export class ConverterDialog extends Dialog {
    private columnNameFixed: boolean = false;

    constructor(protected columnName: string, protected allColumns: string[]) {
        super("Convert column");
        let cn = this.addSelectField("columnName", "Column: ", allColumns, columnName);
        let nk = this.addSelectField("newKind", "Convert to: ", allContentsKind);
        let nn = this.addTextField("newColumnName", "New column name: ", "String");
        cn.onchange = () => this.generateColumnName();
        nk.onchange = () => this.generateColumnName();
        // If the user types a column name don't attempt to change it
        nn.onchange = () => this.columnNameFixed = true;
        this.generateColumnName();
    }

    private generateColumnName(): void {
        if (this.columnNameFixed)
            return;
        let cn = this.getFieldValue("columnName");
        let suffix = " (" + this.getFieldValue("newKind") + ")";
        let nn = cn + suffix;
        if (this.allColumns.indexOf(nn) >= 0) {
            let counter = 0;
            while (this.allColumns.indexOf(nn) >= 0) {
                nn = cn + counter.toString() + suffix;
                counter++;
            }
        }
        this.setFieldValue("newColumnName", nn);
    }
}

/**
 * This class handles type conversions on columns (e.g. String to Integer).
 */
export class ColumnConverter  {
    public static maxCategoricalCount = 1e4;

    constructor(private columnName: string,
        private newKind: ContentsKind,
        private newColumnName: string,
        private table: TableView) {}

    public run(): void {
        if (TableView.allColumnNames(this.table.schema).indexOf(this.newColumnName) >= 0) {
            this.table.reportError(`Column name ${this.newColumnName} already exists in table.`);
            return;
        }
        if (this.newKind == "Category") {
            let rr = this.table.createRpcRequest("hLogLog", this.columnName);
            let rec: HLogLogReceiver = new HLogLogReceiver(this.table.getPage(), rr, this);
            rr.invoke(rec);
        } else {
            this.convert(null);
        }
    }

    public countReceived(count: number, operation: ICancellable): void {
        if (count > ColumnConverter.maxCategoricalCount) {
            this.table.reportError(`Too many values for categorical column. There are ${count}, " +
            "and up to ${ColumnConverter.maxCategoricalCount} are supported.`);
            return;
        }
        this.convert(operation);
    }

    public convert(operation: ICancellable): void {
        let args = {
            colName: this.columnName,
            newColName: this.newColumnName,
            newKind: this.newKind
        };
        let rr = this.table.createRpcRequest("convertColumnMap", args);
        rr.chain(operation);
        rr.invoke(new RemoteTableReceiver(this.table.getPage(), rr));
    }
}

export interface HLogLog {
    distinctItemCount: number
}

class HLogLogReceiver extends OnCompleteRenderer<HLogLog> {
    constructor(page: FullPage, operation: ICancellable, protected cc: ColumnConverter) {
        super(page, operation, "HyperLogLog");
    }

    run(data: HLogLog): void {
        this.cc.countReceived(data.distinctItemCount, this.operation);
    }
}
