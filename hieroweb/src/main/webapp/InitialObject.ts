import {RemoteObject, PartialResult} from "./rpc";
import {TableDataView} from "./table";

export class InitialObject extends RemoteObject {
    public static instance: InitialObject = new InitialObject();

    // The "0" argument is the object id for the initial object.
    // It must match the id of the object declared in RpcServer.java.
    // This is a "well-known" name used for bootstrapping the system.
    private constructor() { super("0"); }

    public loadTable(observer: Rx.Observer<PartialResult<TableDataView>>): void {
        // TODO: add table name argument
        let rr = this.createRpcRequest("loadTable", null);
        rr.invoke(observer);
    }
}
