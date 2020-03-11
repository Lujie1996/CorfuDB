package org.corfudb.logreplication.runtime;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.channel.EventLoopGroup;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.runtime.NodeRouterPool;
import org.corfudb.runtime.RuntimeParameters;
import org.corfudb.runtime.clients.IClientRouter;
import org.corfudb.runtime.clients.LayoutClient;
import org.corfudb.runtime.clients.NettyClientRouter;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.NodeLocator;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadFactory;
import java.util.function.Function;


@Slf4j
public class LogReplicationRuntime {

    /**
     * The parameters used to configure this {@link LogReplicationRuntime}.
     */
    @Getter
    private final RuntimeParameters parameters;

    /**
     * Node Router Pool.
     */
    @Getter
    private NodeRouterPool nodeRouterPool;

    /**
     * The {@link EventLoopGroup} provided to netty routers.
     */
    @Getter
    private final EventLoopGroup nettyEventLoop;


    @Getter
    private LogReplicationClient client;

    public LogReplicationRuntime(@Nonnull RuntimeParameters parameters) {

        // Set the local parameters field
        this.parameters = parameters;

        // Generate or set the NettyEventLoop
        nettyEventLoop =  getNewEventLoopGroup();

        // Initializing the node router pool.
        nodeRouterPool = new NodeRouterPool(getRouterFunction);
    }

    /**
     * Get a new {@link EventLoopGroup} for scheduling threads for Netty. The
     * {@link EventLoopGroup} is typically passed to a router.
     *
     * @return An {@link EventLoopGroup}.
     */
    private EventLoopGroup getNewEventLoopGroup() {
        // Calculate the number of threads which should be available in the thread pool.
        int numThreads = Runtime.getRuntime().availableProcessors() * 2;
        ThreadFactory factory = new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat(parameters.getNettyEventLoopThreadFormat())
                .setUncaughtExceptionHandler(this::handleUncaughtThread)
                .build();
        return parameters.getSocketType().getGenerator().generate(numThreads, factory);
    }

    /**
     * A function to handle getting routers. Used by test framework to inject
     * a test router. Can also be used to provide alternative logic for obtaining
     * a router.
     */
    @Getter
    private final Function<String, IClientRouter> getRouterFunction = (address) -> {
                NodeLocator node = NodeLocator.parseString(address);
                // Generate a new router, start it and add it to the table.
                NettyClientRouter newRouter = new NettyClientRouter(node,
                        getNettyEventLoop(),
                        getParameters());
                log.debug("Connecting to new router {}", node);
                try {
                    newRouter.addClient(new LogReplicationHandler());
                } catch (Exception e) {
                    log.warn("Error connecting to router", e);
                    throw e;
                }
                return newRouter;
            };

    /**
     * Function which is called whenever the runtime encounters an uncaught thread.
     *
     * @param thread    The thread which terminated.
     * @param throwable The throwable which caused the thread to terminate.
     */
    private void handleUncaughtThread(@Nonnull Thread thread, @Nonnull Throwable throwable) {
        if (parameters.getUncaughtExceptionHandler() != null) {
            parameters.getUncaughtExceptionHandler().uncaughtException(thread, throwable);
        } else {
            log.error("handleUncaughtThread: {} terminated with throwable of type {}",
                    thread.getName(),
                    throwable.getClass().getSimpleName(),
                    throwable);
        }
    }

    public void connect (String node) {
        log.info("Connected");
        IClientRouter router = getRouter(node);

        client = new LogReplicationClient(router);
        try {
            Boolean pongReceived = client.ping().get();
            log.info("Pong {}", pongReceived);
        } catch(Exception e) {
            log.error("Pong ERROR", e);
        }
    }

    /**
     * Get a router, given the address.
     *
     * @param address The address of the router to get.
     * @return The router.
     */
    public IClientRouter getRouter(String address) {
        return nodeRouterPool.getRouter(NodeLocator.parseString(address));
    }

}
