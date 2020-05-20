package org.corfudb.logreplication.infrastructure;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.CustomServerRouter;
import org.corfudb.infrastructure.IServerChannelAdapter;
import org.corfudb.runtime.Messages.CorfuMessage;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;

import java.util.concurrent.CompletableFuture;

/**
 * Server GRPC Transport Adapter
 *
 * This router is a default implementation used for transport plugin tests.
 */
@Slf4j
public class GRPCLogReplicationServerChannel extends IServerChannelAdapter {

    /*
     * GRPC Server used for listening and dispatching incoming calls.
     */
    private final Server server;

    /*
     * GRPC Service Stub
     */
    private final GRPCLogReplicationServerHandler service;

    private CompletableFuture<Boolean> serverCompletable;

    public GRPCLogReplicationServerChannel(Integer port, CustomServerRouter adapter) {
        super(port, adapter);
        this.service = new GRPCLogReplicationServerHandler(adapter);
        this.server = ServerBuilder.forPort(port).addService(service).build();
    }

    @Override
    public void send(CorfuMessage msg) {
        service.send(msg);
    }

    @Override
    public CompletableFuture<Boolean> start() {
        try {
            serverCompletable = new CompletableFuture<>();
            server.start();
            log.info("Server started, listening on " + this.getPort());
        } catch (Exception e) {
            log.error("Caught exception while starting server on port {}", getPort(), e);
            throw new UnrecoverableCorfuError(e);
        }

        return serverCompletable;
    }

    @Override
    public void stop() {
        log.info("Stop GRPC service.");
        server.shutdownNow();
        serverCompletable.complete(true);
    }

}
