package dev.rpc.core;

import dev.rpc.protocol.Status;

/**
 * The server's method threw. remoteType() is the class name of what it threw; the exception itself is never
 * rebuilt on the client, since instantiating classes a peer names is how deserialization gadgets work.
 */
public final class RemoteException extends RpcException {

    private static final long serialVersionUID = 1L;

    private final String remoteType;
    private final String remoteMessage;

    public RemoteException(String remoteType, String remoteMessage) {
        super(Status.APPLICATION_ERROR, remoteType + ": " + remoteMessage);
        this.remoteType = remoteType;
        this.remoteMessage = remoteMessage;
    }

    public String remoteType() {
        return remoteType;
    }

    /** The message as the server's exception had it, possibly cut to fit the frame. */
    public String remoteMessage() {
        return remoteMessage;
    }
}
