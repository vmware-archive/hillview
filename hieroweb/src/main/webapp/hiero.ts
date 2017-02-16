import {RpcRequest} from "./rpc";
import {TableView, TableRenderer} from "./table";
import {ScrollBar, ProgressBar} from "./ui";
import {InitialObject, ConsoleErrorReporter} from "./InitialObject";

// Top-level hiero object: exports all typescript classes
// that can be used in html.

// Workaround webpack: export symbols
// by making them fields of the window object.
let public_symbols = {
    RpcRequest,
    TableView,
    ScrollBar,
    ProgressBar,
    InitialObject,
    ConsoleErrorReporter,
    TableRenderer
};

window["hiero"] = public_symbols;
