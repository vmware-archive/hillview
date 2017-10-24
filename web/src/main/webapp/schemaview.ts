import {RemoteTableObjectView} from "./tableData";
import {IDataView, IHtmlElement, FullPage} from "./ui";
import {IColumnDescription, Schema} from "./tableData";


export class SchemaView extends RemoteTableObjectView {
    protected selectedRows: number[];
    protected firstSelectedRow: number;
    protected cellsPerRow: Map<number, HTMLElement[]>;

    constructor(remoteObjectId: string,
                protected page: FullPage,
                public schema: Schema) {
        super(remoteObjectId, page);
        this.topLevel = document.createElement("div");
        //this.selectedRows = new number[];
        //this.cellsPerRow = new Map<number, HTMLElement[]>();

        //let subMenu = new TopSubMenu([ {text: "By Type", action => {}}]);
        //let menu = new TopMenu([ {text: "Filter", subMenu} ]);
        //this.topLevel.appendChild(menu.getHTMLRepresentation());
        //this.topLevel.appendChild(document.createElement("br"));

        let table = document.createElement("table");
        this.topLevel.appendChild(table);
        let tHead = table.createTHead();
        let thr = tHead.appendChild(document.createElement("tr"));
        let thd0 = document.createElement("th");
        thd0.innerHTML = "Number";
        thr.appendChild(thd0);
        let thd1 = document.createElement("th");
        thd1.innerHTML = "Name";
        thr.appendChild(thd1);
        let thd2 = document.createElement("th");
        thd2.innerHTML = "Type";
        thr.appendChild(thd2);

        let tBody = table.createTBody();
        for (let i = 0; i < schema.length; i++) {
            let trow = tBody.insertRow();
            //this.cellsPerRow.set(i, []);
            for (let j = 0; j < 3; j++) {
                let cell = trow.insertCell(j);
                cell.style.textAlign = "right";
                //cell.onclick = e => this.rowClick(i, e);
                if (j == 0)
                    cell.textContent = (i + 1).toString();
                else if (j == 1)
                    cell.textContent = schema[i].name;
                else if (j == 2)
                    cell.textContent = schema[i].kind;
                //this.cellsPerRow.get(i).push(cell);
            }
        }
    }

    refresh(): void {
    }

    /**
     // mouse click on a row
     private rowClick(rowIndex: number, e: MouseEvent): void {
        e.preventDefault();
        if (e.ctrlKey || e.metaKey) {
            this.firstSelectedRow = rowIndex;
            if (this.selectedRows.has(rowIndex))
                this.selectedRows.delete(rowIndex);
            else
                this.selectedRows.add(rowIndex);
        } else if (e.shiftKey) {
            if (this.firstSelectedRow == null)
                this.firstSelectedRow = rowIndex;
            let first = this.firstSelectedRow;
            let last = rowIndex;
            this.selectedRows.clear();
            if (first > last) { let tmp = first; first = last; last = tmp; }
            for (let i = first; i <= last; i++)
                this.selectedRows.add(i);
        } else {
            if (e.button == 2) {
                // right button
                if (this.selectedRows.has(rowIndex))
                    // Do nothing if pressed on a selected column
                    return;
            }

            this.firstSelectedRow = rowIndex;
            this.selectedRows.clear();
            this.selectedRows.add(rowIndex);
        }
        this.highlightSelectedRows();
    }

    private highlightSelectedRows(): void {
        for (let i = 0; i < this.schema.length; i++) {
            if (this.selectedRows.has(i)) {
                for (let j = 0; j < 3; j++)
                    this.cellsPerRow.get(i)[j].classList.add("selected");
            }
        }
    }
     */
}
