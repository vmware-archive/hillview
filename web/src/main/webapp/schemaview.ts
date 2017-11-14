import {ColumnDescription, RecordOrder, RemoteTableObjectView} from "./tableData";
import {FullPage} from "./ui/fullPage";
import {Schema} from "./tableData";
import {StateMachine} from "./stateMachine";
import {SubMenu, TopMenu} from "./ui/menu";
import {NextKList, TableView} from "./table";

/**
 * This class is used to browse through the columns in the schema and select columns from them. It allows the user to
 * view the table consisting of only the selected columns.
 */
export class SchemaView extends RemoteTableObjectView {
    protected selectedRows: StateMachine;
    protected cellsPerRow: Map<number, HTMLElement[]>;

    constructor(remoteObjectId: string,
                protected page: FullPage,
                public schema: Schema,
                private rowCount: number) {
        super(remoteObjectId, page);
        this.topLevel = document.createElement("div");
        this.selectedRows = new StateMachine();
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

    /**
     * This method returns a Schema comprising of the selected columns.
     */
    private createSchema(): Schema {
        let cds: ColumnDescription[] = [];
        this.selectedRows.getStates().forEach(i => {cds.push(this.schema[i])});
        return cds;
    }

    /**
     * This method displays the table consisting of only the columns contained in the schema above.
     */
    private showTable(): void {
        let newPage = new FullPage("Table with selected columns", "Table", this.page);
        this.page.insertAfterMe(newPage);
        let tv = new TableView(this.remoteObjectId, newPage);
        newPage.setDataView(tv);
        let nkl = new NextKList();
        nkl.schema = this.createSchema();
        nkl.rowCount = this.rowCount;
        tv.updateView(nkl, false, new RecordOrder([]), 0);
        tv.scrollIntoView();
    }

    /**
     * This method handles the transitions in the set of selected rows resulting from mouse clicks, combined with
     * various kinds of key selections
     */
    private rowClick(rowIndex: number, e: MouseEvent): void {
        e.preventDefault();
        if (e.ctrlKey || e.metaKey) {
            this.selectedRows.changeState("Ctrl", rowIndex);
        } else if (e.shiftKey) {
            this.selectedRows.changeState("Shift", rowIndex);
        } else
            this.selectedRows.changeState("NoKey", rowIndex);
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
