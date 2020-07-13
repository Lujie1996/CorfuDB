package org.corfudb.integration;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.DataSender;
import org.corfudb.infrastructure.logreplication.replication.LogReplicationSourceManager;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationAckMessage;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.infrastructure.logreplication.replication.send.LogReplicationError;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationQueryMetadataResponse;

import static org.assertj.core.api.Assertions.fail;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
@Data
public class AckDataSender implements DataSender {

    private UUID snapshotSyncRequestId;
    private long baseSnapshotTimestamp;
    private LogReplicationSourceManager sourceManager;
    private ExecutorService channel;

    public AckDataSender() {
        channel = Executors.newSingleThreadExecutor();
    }

    @Override
    public CompletableFuture<LogReplicationAckMessage> send(LogReplicationEntry message) {
        // Emulate it was sent over the wire and arrived on the source side
        // channel.execute(() -> sourceManager.receive(message));
        final CompletableFuture<LogReplicationAckMessage> cf = new CompletableFuture<>();
        LogReplicationAckMessage entry = sourceManager.receive(message);
        if (entry != null) {
            cf.complete(entry);
        }
        return cf;
    }

    @Override
    public CompletableFuture<LogReplicationAckMessage> send(List<LogReplicationEntry> messages) {
        CompletableFuture<LogReplicationAckMessage> ackCF = new CompletableFuture<>();
        messages.forEach(msg -> send(msg));
        return ackCF;
    }

    @Override
    public CompletableFuture<LogReplicationQueryMetadataResponse> sendQueryMetadataRequest() {
        log.warn("Not implemented");
        return null;
    }

    @Override
    public void onError(LogReplicationError error) {
        fail("On Error received for log entry sync");
    }
}
