package org.corfudb.infrastructure.logreplication.replication.send;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.DataSender;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationClient;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationAckMessage;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationQueryMetadataResponse;
import org.corfudb.protocols.wireprotocol.logreplication.MessageType;
import org.corfudb.runtime.Messages;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Slf4j
public class CorfuDataSender implements DataSender {

    private final LogReplicationClient client;

    public CorfuDataSender(LogReplicationClient client) {
        this.client = client;
    }


    @Override
    public CompletableFuture<LogReplicationAckMessage> send(LogReplicationEntry message) {
        log.info("Send single log entry for request {}", message);
        return client.sendLogEntry(message);
    }

    @Override
    public CompletableFuture<LogReplicationAckMessage> send(List<LogReplicationEntry> messages) {
        log.trace("Send multiple log entries [{}] for request {}", messages.size(), messages.get(0).getMetadata().getSyncRequestId());
        CompletableFuture<LogReplicationAckMessage> lastSentMessage = new CompletableFuture<>();
        CompletableFuture<LogReplicationAckMessage> tmp;

        for (LogReplicationEntry message :  messages) {
            tmp = send(message);
            if (message.getMetadata().getMessageMetadataType().equals(MessageType.SNAPSHOT_TRANSFER_END) ||
                    message.getMetadata().getMessageMetadataType().equals(MessageType.LOG_ENTRY_MESSAGE)) {
                lastSentMessage = tmp;
            }
        }

        return lastSentMessage;
    }

    /**
     * Used by Snapshot Full Sync while has done with transferring data and waiting for the receiver to finish applying.
     * The sender queries the receiver's status and will do the proper transition.
     * @return
     */
    @Override
    public CompletableFuture<LogReplicationQueryMetadataResponse> sendQueryMetadataRequest() throws ExecutionException, InterruptedException {
        log.trace("query remote metadata");
        return client.sendQueryMetadataRequest();
    }

    @Override
    public void onError(LogReplicationError error) {

    }
}
