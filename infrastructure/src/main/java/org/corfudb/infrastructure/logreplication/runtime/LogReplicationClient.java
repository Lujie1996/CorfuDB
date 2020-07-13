package org.corfudb.infrastructure.logreplication.runtime;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationAckMessage;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntryMetadata;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationQueryMetadataResponse;
import org.corfudb.runtime.clients.AbstractClient;
import org.corfudb.runtime.clients.IClientRouter;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * A client to send messages to the Log Replication Unit.
 *
 * This class provides access to operations on a remote server
 * for the purpose of log replication.
 *
 * @author amartinezman
 */
@Slf4j
public class LogReplicationClient extends AbstractClient {

    @Getter
    @Setter
    private IClientRouter router;

    public LogReplicationClient(IClientRouter router, String clusterId) {
        super(router, 0, UUID.fromString(clusterId));
        setRouter(router);
    }

    public LogReplicationClient(IClientRouter router, long epoch) {
        super(router, epoch, null);
        setRouter(router);
    }

    public CompletableFuture<LogReplicationQueryMetadataResponse> sendQueryMetadataRequest() {
        return getRouter().sendMessageAndGetCompletable(
                    new CorfuMsg(CorfuMsgType.LOG_REPLICATION_QUERY_METADATA_REQUEST).setEpoch(0));
    }

    public CompletableFuture<LogReplicationAckMessage> sendLogEntry(LogReplicationEntry logReplicationEntry) {
        CorfuMsg msg = new CorfuPayloadMsg<>(CorfuMsgType.LOG_REPLICATION_ENTRY, logReplicationEntry).setEpoch(0);
        return getRouter().sendMessageAndGetCompletable(msg);
    }


   /* public CompletableFuture<LogReplicationAckMessage> sendLogAckMessage(LogReplicationEntryMetadata logReplicationMetadata) {
        LogReplicationAckMessage ackMessage = new LogReplicationAckMessage(logReplicationMetadata);
        CorfuMsg msg = new CorfuPayloadMsg<>(CorfuMsgType.LOG_REPLICATION_ACK_MESSAGE, ackMessage).setEpoch(0);
        return getRouter().sendMessageAndGetCompletable(msg);
    }*/

    @Override
    public void setRouter(IClientRouter router) {
        this.router = router;
    }

    @Override
    public IClientRouter getRouter() {
        return router;
    }

}
