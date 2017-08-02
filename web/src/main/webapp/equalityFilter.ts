import {TableView, ColumnDescription} from "./table";
import {Dialog} from "./dialog"

// Class explaining the search we want to perform
export class EqualityFilterDescription {
    columnDescription: ColumnDescription;
    compareValue: string;
    complement: boolean;
}

// Dialog that has fields for making an EqualityFilterDescription.
export class EqualityFilterDialog extends Dialog {
    constructor(
        private columnDescription: ColumnDescription,
        private rrCallback: (filter: EqualityFilterDescription) => void,
    ) {
        super("Search");
        this.addTextField("query", "Compare:", columnDescription.kind);
        this.addSelectField("complement", "Check for:", ["Equality", "Inequality"]);
    }

    protected confirmAction(): void {
        let textQuery: string = this.fields["query"].html.value;
        let complement = this.fields["complement"].html.value == "Inequality";

        let filter: EqualityFilterDescription = {
            columnDescription: this.columnDescription,
            compareValue: textQuery,
            complement: complement,
        };

        // Call the function that invokes the RPC.
        this.rrCallback(filter);
        
        this.container.remove();
    }
}