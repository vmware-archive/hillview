// RemoteObject.ts: implementation of objects that have part of their state on
// a remote machine, and where some methods are dispatched to the remote machine.

export class RemoteObject {
    constructor(public remoteObjectId : string) {}
}