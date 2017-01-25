import {ContentsKind, TableView, SchemaView, TableDataView} from "./table";
import {RemoteObject, Callback} from "./rpc";
import Observer = Rx.Observer;

let schemaJson : SchemaView = [{
        kind: ContentsKind.String,
        name: "city",
        sortOrder: -1
    },
    {
        kind: ContentsKind.Integer,
        name: "zipcode",
        sortOrder: 2
    },
    {
        kind: ContentsKind.Date,
        name: "when",
        sortOrder: 0
    }
];

export let tableJson1 : TableDataView = {
    schema: schemaJson,
    rows: [
        { count: 1, values: [ "Sunnyvale", 94087 ] },
        { count: 3, values: [ "Mountain View", 94043 ] }
    ],
    rowCount: 10,
    startPosition: 0
};

export let tableJson2 : TableDataView = {
    schema: schemaJson,
    rows: [
        { count: 1, values: [ "Sunnyvale", 94087 ] },
        { count: 3, values: [ "Mountain View", 94043 ] },
        { count: 1, values: [ "Palo Alto", 94304 ] }
    ],
    rowCount : 10,
    startPosition : 0
};

export function createTable() : TableView {
    return new TableView();
}

export class InitialObject extends RemoteObject {
    private constructor() {
        super("*");
    }

    public static readonly instance : InitialObject = new InitialObject();
}