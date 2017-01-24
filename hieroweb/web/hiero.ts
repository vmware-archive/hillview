import {RpcRequest} from "./rpc";
import {TableView} from "./table";
import {ScrollBar, ProgressBar} from "./ui";
import {createTable, tableJson2, tableJson1} from "./test";

// Workaround webpack: export symbols
// by making them fields of the window object.
let public_symbols = {
    RpcRequest,
    TableView,
    ScrollBar,
    ProgressBar,
    createTable,
    tableJson1,
    tableJson2
};

window["hiero"] = public_symbols;
