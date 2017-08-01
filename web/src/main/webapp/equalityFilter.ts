import {TableView} from "./table";

export class EqualityFilterDescription {
    columnName: string;
    compareValue: string;
    complement: boolean;
}

export class EqualityFilterDialog {
	private filter: EqualityFilterDescription;

	constructor(
		private columnName: string,
		private callback: (filter: EqualityFilterDescription) => void
	) {
		// Create a hand-coded filter and apply it for now.
		this.filter = {
            columnName: this.columnName,
            compareValue: "CA",
            complement: false,
        };
		this.apply();
	}

	private apply(): void {
        this.callback(this.filter);
	}
}