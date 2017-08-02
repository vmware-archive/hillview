import {TableView, ColumnDescription} from "./table";
import {Dialog} from "./dialog"

export class EqualityFilterDescription {
    columnDescription: ColumnDescription;
    compareValue: string;
    complement: boolean;
}

export class EqualityFilterDialog extends Dialog {
	constructor(
		private columnDescription: ColumnDescription,
		private rrCallback: (filter: EqualityFilterDescription) => void,
	) {
		super("Equality filter");
		this.addTextField("query", "Query for:", columnDescription.kind);
		this.addSelectField("complement", "Check for:", ["Equality", "Inequality"]);
	}

	protected confirm(): void {
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