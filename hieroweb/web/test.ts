import {ContentsKind, TableView, SchemaView} from "./table";

let schemaJson : SchemaView = [{
        kind: ContentsKind.String,
        name: "city",
        allowMissing: false,
        sortInfo: -1
    },
    {
        kind: ContentsKind.Integer,
        name: "zipcode",
        allowMissing: false,
        sortInfo: 2
    },
    {
        kind: ContentsKind.Date,
        name: "when",
        allowMissing: false,
        sortInfo: 0
    }
];

let tableJson = {
    schema: schemaJson,
    rows: [
        [ "Sunnyvale", 94087 ],
        [ "Mountain View", 94043 ]
    ]
};

export function createTable() : TableView {
    let tbl = new TableView(tableJson);
    tbl.setScroll(0.1, 0.3);
    return tbl;
}