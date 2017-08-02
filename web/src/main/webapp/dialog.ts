import {IHtmlElement} from "./ui"
import {ContentsKind, ColumnDescription} from "./table"
import {Pair} from "./util"

export class DialogField {
	html: HTMLSelectElement | HTMLInputElement;
	type?: ContentsKind;
}

export abstract class Dialog implements IHtmlElement {
	protected container: HTMLDivElement;

	// Stores the input elements and (optionally) their types.
	protected fields: {[fieldName: string]: DialogField} = {};

	constructor(title: string) {
		this.container = document.createElement("div");
		this.container.classList.add('dialog');
		document.body.appendChild(this.container);

		let titleElement = document.createElement("h1");
		titleElement.textContent = title;
		this.container.appendChild(titleElement); 

		let buttonsDiv = document.createElement("div");
		this.container.appendChild(buttonsDiv);
		buttonsDiv.classList.add('cancelconfirm');

		let cancelButton = document.createElement("button");
		cancelButton.onclick = () => this.cancel();
		cancelButton.textContent = "Cancel";
		cancelButton.classList.add("cancel");
		buttonsDiv.appendChild(cancelButton);

		let confirmButton = document.createElement("button");
		confirmButton.onclick = () => this.confirm();
		confirmButton.textContent = "Confirm";
		confirmButton.classList.add("confirm");
		buttonsDiv.appendChild(confirmButton);
	}

	public getHTMLRepresentation(): HTMLDivElement {
		return this.container;
	}

	protected addTextField(fieldName: string, labelText: string, type: ContentsKind): void {
		let fieldDiv = document.createElement("div");
		this.container.appendChild(fieldDiv);

		let label = document.createElement("label");
		label.textContent = labelText;
		fieldDiv.appendChild(label);

		let input = <HTMLInputElement> document.createElement("input");
		fieldDiv.appendChild(input);
		if (type == "Integer") {
			input.type = "number";
		}
		this.fields[fieldName] = {html: input, type: type};
	}

	protected addSelectField(fieldName: string, labelText: string, options: string[]): void {
		let fieldDiv = document.createElement("div");
		this.container.appendChild(fieldDiv);

		let label = document.createElement("label");
		label.textContent = labelText;
		fieldDiv.appendChild(label);

		let select = document.createElement("select");
		fieldDiv.appendChild(select);
		options.forEach(option => {
			let optionElement = document.createElement("option");
			optionElement.value = option;
			optionElement.text = option;
			select.add(optionElement);
		})
		this.fields[fieldName] = {html: select}
	}

	private cancel(): void {
		this.container.remove();
	}

	protected abstract confirm(): void;
}