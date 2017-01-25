import {IHtmlElement, ScrollBar} from "./ui";

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

export interface IColumnDescriptionView {
    readonly kind: ContentsKind;
    readonly name: string;
    readonly sortOrder: number;  // 0 - not visible, >0 - ascending, <0 - descending
}

export class ColumnDescriptionView implements IColumnDescriptionView {
    readonly kind: ContentsKind;
    readonly name: string;
    readonly sortOrder: number;  // 0 - not visible, >0 - ascending, <0 - descending

    constructor(v : IColumnDescriptionView) {
        this.kind = v.kind;
        this.name = v.name;
        this.sortOrder = v.sortOrder;
    }

    // If something is not sorted, it is not visible
    public isVisible(): boolean {
        return this.sortOrder != 0;
    }
    public isAscending(): boolean {
        return this.sortOrder > 0;
    }
    public getSortIndex(): number {
        return this.sortOrder < 0 ? -this.sortOrder : this.sortOrder;
    }
    public getSortArrow() : string {
        if (this.isAscending())
            return "&dArr;";
        else
            return "&uArr;";
    }
}

export interface SchemaView {
    [index: number] : IColumnDescriptionView;
    length: number;
}

export interface RowView {
    count: number;
    values: any[];
}

export interface TableDataView {
    schema: SchemaView;
    rowCount: number;
    startPosition: number;
    rows?: RowView[];
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

export class TableView implements IHtmlElement {
    protected schema: SchemaView;
    protected top : HTMLDivElement;
    protected scrollBar : ScrollBar;
    protected htmlTable : HTMLTableElement;
    protected thead : HTMLTableSectionElement;
    protected tbody: HTMLTableSectionElement;
    protected elementCount: number;
    protected startPosition: number;

    constructor() {
        this.top = document.createElement("div");
        this.htmlTable = document.createElement("table");
        this.top.className = "flexcontainer";
        this.scrollBar = new ScrollBar();
        this.top.appendChild(this.htmlTable);
        this.top.appendChild(this.scrollBar.getHTMLRepresentation());
    }

    private static addHeaderCell(thr: Node, cd: ColumnDescriptionView) : void {
        let thd = document.createElement("th");
        let label = cd.name;
        if (!cd.isVisible()) {
            thd.className = "hidden";
        } else {
            label += " " +
                cd.getSortArrow() + cd.getSortIndex();
        }
        thd.innerHTML = label;
        thr.appendChild(thd);
    }

    public updateView(data : TableDataView) : void {
        this.elementCount = 0;
        this.startPosition = data.startPosition;
        this.schema = data.schema;

        if (this.thead != null) {
            this.thead.remove();
            this.tbody.remove();
        }
        this.thead = this.htmlTable.createTHead();
        let thr = this.thead.appendChild(document.createElement("tr"));

        let cds : ColumnDescriptionView[] = [];
        let poscd = new ColumnDescriptionView({ kind: ContentsKind.Integer, name: ":position", sortOrder: 0 });
        let ctcd = new ColumnDescriptionView({ kind: ContentsKind.Integer, name: ":count", sortOrder: 0 });

        TableView.addHeaderCell(thr, poscd);
        TableView.addHeaderCell(thr, ctcd);
        for (let i = 0; i < this.schema.length; i++) {
            let cd = new ColumnDescriptionView(this.schema[i]);
            cds.push(cd);
            TableView.addHeaderCell(thr, cd);
        }
        this.tbody = this.htmlTable.createTBody();

        if (data.rows != null) {
            for (let i = 0; i < data.rows.length; i++)
                this.addRow(data.rows[i], cds);
        }
        this.setScroll(data.startPosition / data.rowCount,
            (data.startPosition + this.elementCount) / data.rowCount);
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

    public addRow(row : RowView, cds: ColumnDescriptionView[]) : void {
        let trow = this.tbody.insertRow();
        let dataIndex : number = 0;

        let cell = trow.insertCell(0);
        cell.className = "rightAlign";
        cell.textContent = String(this.startPosition + this.elementCount);

        cell = trow.insertCell(1);
        cell.className = "rightAlign";
        cell.textContent = String(row.count);

        for (let i = 0; i < cds.length; i++) {
            let cd = cds[i];
            cell = trow.insertCell(i + 2);
            if (cd.isVisible()) {
                cell.className = "rightAlign";
                cell.textContent = String(row.values[dataIndex]);
                dataIndex++;
            }
        }
        this.elementCount += row.count;
    }

    public setScroll(top: number, bottom: number) : void {
        this.scrollBar.setPosition(top, bottom);
    }
}