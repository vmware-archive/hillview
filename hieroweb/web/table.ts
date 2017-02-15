import {IHtmlElement, ScrollBar, Menu, Renderer, FullPage, HieroDataView} from "./ui";
import {RemoteObject, PartialResult, RpcReceiver} from "./rpc";
import Rx = require('rx');

// These classes are direct counterparts to server-side Java classes
// with the same names.  JSON serialization
// of the Java classes produces JSON that can be directly cast
// into these interfaces.
export enum ContentsKind {
    String,
    Integer,
    Double,
    Date,
    Interval
}

export interface IColumnDescription {
    readonly kind: ContentsKind;
    readonly name: string;
    readonly allowMissing: boolean;
}

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

export interface Schema {
    [index: number] : IColumnDescription;
    length: number;
}

export interface RowView {
    count: number;
    values: any[];
}

export interface ColumnSortOrientation {
    columnDescription: IColumnDescription;
    ascending: boolean;
}

export interface RecordOrder {
    length: number;
    [index: number]: ColumnSortOrientation;
}

export interface TableDataView {
    schema?: Schema;
    // Total number of rows in the complete table
    rowCount: number;
    startPosition?: number;
    rows?: RowView[];
    order?: RecordOrder;
}

/* Example table view:
-------------------------------------------
| pos | count | col0 v1 | col1 ^0 | col2 |
-------------------------------------------
| 10  |     3 | Mike    |       0 |      |
 ------------------------------------------
 | 13 |     6 | Jon     |       1 |      |
 ------------------------------------------
 */

export class TableView extends RemoteObject
    implements IHtmlElement, HieroDataView {
    // Data view part: received from remote site
    protected schema?: Schema;
    // Logical position of first row displayed
    protected startPosition?: number;
    // Total rows in the table
    protected rowCount?: number;
    protected order?: RecordOrder;
    // Computed
    // Logical number of data rows displayed; includes count of each data row
    protected dataRowsDisplayed: number;
    // HTML part
    protected top : HTMLDivElement;
    protected scrollBar : ScrollBar;
    protected htmlTable : HTMLTableElement;
    protected thead : HTMLTableSectionElement;
    protected tbody: HTMLTableSectionElement;
    protected page: FullPage;

    public constructor(id: string) {
        super(id);
        this.top = document.createElement("div");
        this.htmlTable = document.createElement("table");
        this.top.className = "flexcontainer";
        this.scrollBar = new ScrollBar();
        this.top.appendChild(this.htmlTable);
        this.top.appendChild(this.scrollBar.getHTMLRepresentation());
    }

    setPage(page: FullPage) {
        this.page = page;
    }

    getPage() : FullPage {
        return this.page;
    }

    getSortOrder(column: string): [boolean, number] {
        if (this.order == null)
            return null;
        for (let i = 0; i < this.order.length; i++) {
            let o = this.order[i];
            if (o.columnDescription.name == column)
                return [o.ascending, i];
        }
        return null;
    }

    public isVisible(column: string): boolean {
        let so = this.getSortOrder(column);
        return so != null;
     }

    public isAscending(column: string): boolean {
        let so = this.getSortOrder(column);
        if (so == null) return null;
        return so[0];
    }

    public getSortIndex(column: string): number {
        let so = this.getSortOrder(column);
        if (so == null) return null;
        return so[1];
    }

    public getSortArrow(column: string): string {
        let asc = this.isAscending(column);
        if (asc == null)
            return "";
        else if (asc)
            return "&dArr;";
        else
            return "&uArr;";
    }

    private addHeaderCell(thr: Node, cd: ColumnDescription) : HTMLElement {
        let thd = document.createElement("th");
        let label = cd.name;
        if (!this.isVisible(cd.name)) {
            thd.className = "hiddenColumn";
        } else {
            label += " " +
                this.getSortArrow(cd.name) + this.getSortIndex(cd.name);
        }
        thd.innerHTML = label;
        thr.appendChild(thd);
        return thd;
    }

    public showColumn(columnName: string, show: boolean) : void {
        let rr = this.createRpcRequest("show", null);
        //rr.invoke();
    }

    public updateView(data: TableDataView) : void {
        this.dataRowsDisplayed = 0;
        this.startPosition = data.startPosition;
        this.rowCount = data.rowCount;
        this.schema = data.schema;
        this.order = data.order;

        if (this.thead != null)
            this.thead.remove();
        if (this.tbody != null)
            this.tbody.remove();
        this.thead = this.htmlTable.createTHead();
        let thr = this.thead.appendChild(document.createElement("tr"));

        // These two columns are always shown
        let cds : ColumnDescription[] = [];
        let poscd = new ColumnDescription({
            kind: ContentsKind.Integer,
            name: "(position)",
            allowMissing: false });
        let ctcd = new ColumnDescription({
            kind: ContentsKind.Integer,
            name: "(count)",
            allowMissing: false });

        // Create column headers
        this.addHeaderCell(thr, poscd);
        this.addHeaderCell(thr, ctcd);
        if (this.schema == null)
            return;

        for (let i = 0; i < this.schema.length; i++) {
            let cd = new ColumnDescription(this.schema[i]);
            cds.push(cd);
            let thd = this.addHeaderCell(thr, cd);
            let menu = new Menu([
                {text: "show", action: () => this.showColumn(cd.name, true) },
                {text: "hide", action: () => this.showColumn(cd.name, false)}
             ]);
            thd.onclick = () => menu.toggleVisibility();
            thd.appendChild(menu.getHTMLRepresentation());
        }
        this.tbody = this.htmlTable.createTBody();

        // Add row data
        if (data.rows != null) {
            for (let i = 0; i < data.rows.length; i++)
                this.addRow(data.rows[i], cds);
        }

        // Create table footer
        let footer = this.tbody.insertRow();
        let cell = footer.insertCell(0);
        cell.colSpan = this.schema.length + 2;
        cell.className = "footer";
        cell.textContent = String(this.rowCount + " rows");

        this.updateScrollBar();
    }

    private updateScrollBar(): void {
        if (this.startPosition == null || this.rowCount == null)
            return;
        this.setScroll(this.startPosition / this.rowCount,
            (this.startPosition + this.dataRowsDisplayed) / this.rowCount);
    }

    public getRowCount() : number {
        return this.tbody.childNodes.length;
    }

    public getColumnCount() : number {
        return this.schema.length;
    }

    public getHTMLRepresentation() : HTMLElement {
        return this.top;
    }

    public addRow(row : RowView, cds: ColumnDescription[]) : void {
        let trow = this.tbody.insertRow();
        let dataIndex : number = 0;

        let cell = trow.insertCell(0);
        cell.className = "rightAlign";
        cell.textContent = String(this.startPosition + this.dataRowsDisplayed);

        cell = trow.insertCell(1);
        cell.className = "rightAlign";
        cell.textContent = String(row.count);

        for (let i = 0; i < cds.length; i++) {
            let cd = cds[i];
            cell = trow.insertCell(i + 2);
            if (this.isVisible(cd.name)) {
                cell.className = "rightAlign";
                cell.textContent = String(row.values[dataIndex]);
                dataIndex++;
            }
        }
        this.dataRowsDisplayed += row.count;
    }

    public setScroll(top: number, bottom: number) : void {
        this.scrollBar.setPosition(top, bottom);
    }
}

export class TableRenderer extends Renderer<TableDataView> {
    constructor(page: FullPage, protected table: TableView) {
        super(page.progressManager.newProgressBar("Get info"), page.getErrorReporter());
    }

    onNext(value: PartialResult<TableDataView>): void {
        this.progress.setPosition(value.done);
        this.table.updateView(value.data);
    }
}

export class RemoteTableReceiver extends RpcReceiver<string> {
    public table: TableView;

    constructor(protected page: FullPage) {
        super(page.getErrorReporter());
    }

    private retrieveSchema(): void {
        let rr = this.table.createRpcRequest("getSchema", null);
        rr.invoke(new TableRenderer(this.page, this.table));
    }

    public onNext(value: string): void {
        this.table = new TableView(value);
        this.page.setHieroDataView(this.table);
        this.retrieveSchema();
    }
}
