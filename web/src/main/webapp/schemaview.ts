import {ColumnDescription, RecordOrder, RemoteTableObjectView} from "./tableData";
import {FullPage} from "./ui/fullPage";
import {Schema} from "./tableData";
import {SubMenu, TopMenu} from "./ui/menu";
import {NextKList, TableView} from "./table";
import {TabularDisplay} from "./ui/tabularDisplay";

/**
 * This class is used to browse through the columns in the schema and select columns from them.
 */
export class SchemaView extends RemoteTableObjectView {
    protected display: TabularDisplay;

    constructor(remoteObjectId: string,
                protected page: FullPage,
                public schema: Schema,
                private rowCount: number) {
        super(remoteObjectId, page);
        this.topLevel = document.createElement("div");

        this.topLevel = document.createElement("div");
        let subMenu = new SubMenu([
            {text: "Selected columns", action: () => {this.showTable();}}
        ]);
        let menu = new TopMenu([{text: "View", subMenu}]);
        this.page.setMenu(menu);
        this.topLevel.appendChild(document.createElement("br"));

        this.display = new TabularDisplay();
        this.display.setColumns(["#", "Name", "Type", "Allows missing"]);

        for (let i = 0; i < schema.length; i++)
            this.display.addRow([(i+1).toString(), schema[i].name,
                schema[i].kind.toString(), schema[i].allowMissing.toString()]);
        this.topLevel.appendChild(this.display.getHTMLRepresentation());
    }

    refresh(): void { }

    /**
     * This method returns a Schema comprising of the selected columns.
     */
    private createSchema(): Schema {
        let cds: ColumnDescription[] = [];
        this.display.getSelectedRows().forEach(i => { cds.push(this.schema[i]) });
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


}
