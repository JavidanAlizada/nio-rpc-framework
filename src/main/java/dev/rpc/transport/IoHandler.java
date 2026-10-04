package dev.rpc.transport;

/** Attached to a selection key; told which operations are ready. Always called on the owning loop's thread. */
interface IoHandler {

    void onReady(int readyOps);
}
