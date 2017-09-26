/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
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

import {Dialog} from "./dialog";
import {ContentsKind, asContentsKind} from "./tableData";
import {TableView, RemoteTableReceiver} from "./table";
import {Renderer} from "./rpc";
import {PartialResult, ICancellable} from "./util";
import {FullPage} from "./ui";

export class ColumnConverter  {
    public static maxCategoricalCount = 1e4;

    public static dialog(columnName: string, allColumns: string[], table: TableView) {
        let dialog: Dialog = new Dialog("Convert column");
        dialog.addSelectField("columnName", "Column: ", allColumns, columnName);
        dialog.addSelectField("newKind", "Convert to: ", ["Category", "Json", "String", "Integer", "Double", "Date", "Interval"]);
        dialog.addTextField("newColumnName", "New column name: ", "String", columnName + " (Cat.)");
        dialog.setAction(() => {
            let kind: ContentsKind = asContentsKind(dialog.getFieldValue("newKind"));
            let converter: ColumnConverter = new ColumnConverter(
                dialog.getFieldValue("columnName"),
                kind,
                dialog.getFieldValue("newColumnName"),
                table
            );
            converter.run();
        });
        dialog.show();
    }

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
            rr.invoke(new HLogLogReceiver(this.table.getPage(), rr, "HLogLog",
                (count) => this.checkValidForCategory(count)));
        } else {
            this.runConversion();
        }
    }

    private checkValidForCategory(hLogLog: HLogLog) {
        if (hLogLog.distinctItemCount > ColumnConverter.maxCategoricalCount) {
            this.table.reportError(`Too many values for categorical column. There are ${hLogLog.distinctItemCount}, and up to ${ColumnConverter.maxCategoricalCount} are supported.`);
        } else {
            this.runConversion();
        }
    }

    private runConversion() {
        let args = {
            colName: this.columnName,
            newColName: this.newColumnName,
            newKind: this.newKind
        };
        let rr = this.table.createRpcRequest("convertColumnMap", args);
        rr.invoke(new RemoteTableReceiver(this.table.getPage(), rr));
    }
}

interface HLogLog {
    distinctItemCount: number
}

export class HLogLogReceiver extends Renderer<HLogLog> {
    private data: HLogLog;

    constructor(page: FullPage, operation: ICancellable, name: string, private next: (number) => void) {
        super(page, operation, name);
        this.data = null;
    }

    onNext(value: PartialResult<HLogLog>) {
        super.onNext(value);
        this.data = value.data;
    }

    onCompleted(): void {
        super.onCompleted();
        if (this.data != null)
            this.next(this.data)
    }
}
