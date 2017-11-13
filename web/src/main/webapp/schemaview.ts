import {ColumnDescription, RecordOrder, RemoteTableObjectView} from "./tableData";
import {FullPage} from "./ui";
import {Schema} from "./tableData";
import {stateMachine} from "./stateMachine";
import {SubMenu, TopMenu} from "./menu";
import {NextKList, TableView} from "./table";

export class SchemaView extends RemoteTableObjectView {
    protected selectedRows: stateMachine;
    protected cellsPerRow: Map<number, HTMLElement[]>;

    constructor(remoteObjectId: string,
                protected page: FullPage,
                public schema: Schema,
                private rowCount: number) {
        super(remoteObjectId, page);
        this.topLevel = document.createElement("div");
        this.selectedRows = new stateMachine();
        this.cellsPerRow = new Map<number, HTMLElement[]>();

        this.topLevel = document.createElement("div");
        let subMenu = new SubMenu([
            {text: "As Table", action: () => {this.showTable();}}
        ]);
        let menu = new TopMenu([{text: "View Table", subMenu}]);
        this.topLevel.appendChild(menu.getHTMLRepresentation());
        this.topLevel.appendChild(document.createElement("br"));

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
            this.cellsPerRow.set(i, []);
            for (let j = 0; j < 3; j++) {
                let cell = trow.insertCell(j);
                cell.style.textAlign = "right";
                cell.onclick = e => this.rowClick(i, e);
                if (j == 0)
                    cell.textContent = (i + 1).toString();
                else if (j == 1)
                    cell.textContent = schema[i].name;
                else if (j == 2)
                    cell.textContent = schema[i].kind;
                this.cellsPerRow.get(i).push(cell);
            }
        }
    }

    refresh(): void {
    }

    private createSchema(): Schema {
        let cds: ColumnDescription[] = [];
        this.selectedRows.getStates().forEach(i => {cds.push(this.schema[i])});
        return cds;
    }
    private showTable(): void {
        let newPage = new FullPage("Table of Selected Columns", this.page);
        this.page.insertAfterMe(newPage);
        let tv = new TableView(this.remoteObjectId, newPage);
        newPage.setDataView(tv);
        let nkl = new NextKList();
        nkl.schema = this.createSchema();
        nkl.rowCount = this.rowCount;
        tv.updateView(nkl, false, new RecordOrder([]), 0);
        tv.scrollIntoView();
    }

    // mouse click on a row
    private rowClick(rowIndex: number, e: MouseEvent): void {
        e.preventDefault();
        if (e.ctrlKey || e.metaKey) {
            this.selectedRows.changeState(1, rowIndex);
        } else if (e.shiftKey) {
            this.selectedRows.changeState(2, rowIndex);
        } else
            this.selectedRows.changeState(0, rowIndex);
        this.highlightSelectedRows();
    }

    private highlightSelectedRows(): void {
        for (let i = 0; i < this.schema.length; i++) {
            if (this.selectedRows.has(i)) {
                for (let j = 0; j < 3; j++)
                    this.cellsPerRow.get(i)[j].classList.add("selected");
            }
            else {
                for (let j = 0; j < 3; j++)
                    this.cellsPerRow.get(i)[j].classList.remove("selected");
            }
        }
    }
}
