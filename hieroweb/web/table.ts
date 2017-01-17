import {IHtmlElement, ScrollBar} from "./ui";

export enum ContentsKind {
    String,
    Integer,
    Double,
    Date,
    Interval
}

export class SortInfo {
    // If something is not sorted, it is not visible
    public static isVisible(sortInfo: number): boolean {
        return sortInfo != 0;
    }
    public static isAscending(sortInfo: number): boolean {
        return sortInfo > 0;
    }
    public static getSortIndex(sortInfo: number): number {
        return sortInfo < 0 ? -sortInfo : sortInfo;
    }
    public static getSortArrow(sortInfo: number) : string {
        if (SortInfo.isAscending(sortInfo))
            return "&dArr;";
        else
            return "&uArr;";
    }
}

export interface ColumnDescriptionView {
    readonly kind: ContentsKind;
    readonly name: string;
    readonly allowMissing: boolean;
    readonly sortInfo: number;
}

export interface SchemaView {
    [index: number] : ColumnDescriptionView;
    length: number;
}

export interface IRow {
    [index: number]: any;
    length: number;
}

interface ITableData {
    schema: SchemaView;
    rows?: IRow[];
}

export class TableView implements IHtmlElement {
    readonly schema: SchemaView;

    protected top : HTMLDivElement;
    protected scrollBar : ScrollBar;
    protected htmlTable : HTMLTableElement;
    protected thead : HTMLTableSectionElement;
    protected tbody: HTMLTableSectionElement;

    constructor(data: ITableData) {
        this.schema = data.schema;

        this.top = document.createElement("div");
        this.htmlTable = document.createElement("table");
        this.top.className = "flexcontainer";
        this.scrollBar = new ScrollBar();
        this.top.appendChild(this.htmlTable);
        this.top.appendChild(this.scrollBar.getHTMLRepresentation());
        this.thead = this.htmlTable.createTHead();
        let thr = this.thead.appendChild(document.createElement("tr"));
        for (let i = 0; i < this.schema.length; i++) {
            let cd = this.schema[i];
            let thd = document.createElement("th");
            let label = cd.name;
            if (!SortInfo.isVisible(cd.sortInfo)) {
                thd.className = "hidden";
            } else {
                label += " " +
                    SortInfo.getSortArrow(cd.sortInfo) + SortInfo.getSortIndex(cd.sortInfo);
            }
            thd.innerHTML = label;
            thr.appendChild(thd);
        }
        this.tbody = this.htmlTable.createTBody();

        if (data.rows != null) {
            for (let i = 0; i < data.rows.length; i++)
                this.addRow(data.rows[i]);
        }
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

    public addRow(r : IRow) : void {
        let row = this.tbody.insertRow();
        let dataIndex : number = 0;
        for (let i = 0; i < this.schema.length; i++) {
            let cd = this.schema[i];
            let cell = row.insertCell(i);
            if (SortInfo.isVisible(cd.sortInfo)) {
               cell.className = "rightAlign";
                cell.textContent = String(r[dataIndex]);
                dataIndex++;
            }
        }
    }

    public setScroll(top: number, bottom: number) : void {
        this.scrollBar.setPosition(top, bottom);
    }
}