import {RpcRequest} from "./rpc";
import {RemoteObject} from "./remoteObject";
import {TableView} from "./table";
import {ScrollBar, ProgressBar} from "./ui";
import {createTable} from "./test";

// Workaround webpack: export symbols
// by making them fields of the window object.
let public_symbols = {
    RpcRequest,
    TableView,
    ScrollBar,
    ProgressBar,
    createTable
};

window["hiero"] = public_symbols;
