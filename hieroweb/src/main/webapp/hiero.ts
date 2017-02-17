import {TableView, RemoteTableReceiver} from "./table";
import {ScrollBar, ProgressBar, DataDisplay, FullPage} from "./ui";
import {InitialObject} from "./InitialObject";
import {ConsoleErrorReporter} from "./errorReporter";

// Top-level hiero object: exports all typescript classes
// that can be used in html.

// Workaround webpack: export symbols
// by making them fields of the window object.
let public_symbols = {
    TableView,
    ScrollBar,
    ProgressBar,
    InitialObject,
    ConsoleErrorReporter,
    RemoteTableReceiver,
    DataDisplay,
    FullPage
};

window["hiero"] = public_symbols;
