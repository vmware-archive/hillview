import {RpcRequest} from "./rpc";

function inc(a : number) : number { return a + 1; }
const person = "Mihai";

// Workaround webpack: export symbols
// by making them fields of the window object.
var public_symbols = {
    inc,
    person,
    RpcRequest
};

window["hiero"] = public_symbols;
