
import {Dialog} from "../dialog";
import {TableView, Schema, isNumeric, isCategorical} from "../table";
import {HeatMapArrayView} from "./view";
import {HeatMapArrayArgs} from "./types";
import {FullPage} from "../ui";

// This class shows the dialog and takes care of initiating the right sketches.
export class HeatMapArrayDialog extends Dialog {
    private heatMapArrayView: HeatMapArrayView;

    constructor(selectedColumns: string[], private page: FullPage, private schema: Schema, remoteObjectId: string) {
        super("Heat map array")

        // Fields for the heat map.
        this.addSelectField("col1", "Heat map column 1: ", selectedColumns, selectedColumns[0]);
        this.addSelectField("col2", "Heat map column 2: ", selectedColumns, selectedColumns[1]);
        this.addSelectField("col3", "Array column: ", selectedColumns, selectedColumns[2]);
        this.setAction(() => {
            let args = this.parseFields();
            this.heatMapArrayView = new HeatMapArrayView(remoteObjectId, page, args);
        });
    }

    private parseFields(): HeatMapArrayArgs {
        let cd1 = TableView.findColumn(this.schema, this.getFieldValue("col1"));
        let cd2 = TableView.findColumn(this.schema, this.getFieldValue("col2"));
        let cd3 = TableView.findColumn(this.schema, this.getFieldValue("col3"));
        return {
            cds: [cd1, cd2, cd3],
        };
    }



}
