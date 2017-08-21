export class PCAProjectionRequest {
	columnNames: string[]
	constructor(colNames: Set<string>) {
		this.columnNames = [];
		colNames.forEach((col) => this.columnNames.push(col));
	}
};