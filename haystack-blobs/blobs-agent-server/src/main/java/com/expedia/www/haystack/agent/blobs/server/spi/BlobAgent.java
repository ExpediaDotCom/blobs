package com.expedia.www.haystack.agent.blobs.server.spi;

import com.expedia.www.haystack.agent.blobs.dispatcher.core.BlobDispatcher;
import com.expedia.www.haystack.agent.blobs.server.api.BlobAgentGrpcServer;
import com.expedia.www.haystack.agent.core.Agent;
import com.google.common.annotations.VisibleForTesting;
import com.typesafe.config.Config;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

public class BlobAgent implements Agent {
    private static Logger LOGGER = LoggerFactory.getLogger(BlobAgent.class);
    private final static String MAX_BLOB_SIZE_KB = "max.blob.size.in.kb";
    private final static String DISPATCHERS = "dispatchers";
    private final static String PORT = "port";

    private List<BlobDispatcher> dispatchers;
    private Server server;

    @VisibleForTesting
    BlobAgent(List<BlobDispatcher> dispatchers, Server server) {
        this.dispatchers = dispatchers;
        this.server = server;
    }

    public BlobAgent(){

    }

    @Override
    public String getName() {
        return "ossblobs";
    }

    @Override
    public void initialize(Config config) throws Exception {
        dispatchers = loadAndInitializeDispatchers(config, Thread.currentThread().getContextClassLoader());

        _initialize(dispatchers, config);
    }

    void _initialize(List<BlobDispatcher> dispatchers, Config config) throws IOException {
        Validate.isTrue(config.hasPath(MAX_BLOB_SIZE_KB), "max message size for blobs needs to be specified");
        final Integer maxBlobSizeInKB = config.getInt(MAX_BLOB_SIZE_KB);

        Validate.isTrue(config.hasPath(PORT), "port for service needs to be specified");
        final Integer port = config.getInt(PORT);

        final int maxBlobSizeInBytes = maxBlobSizeInKB * 1024;
        final NettyServerBuilder builder = NettyServerBuilder
                .forPort(port)
                .directExecutor()
                .addService(new BlobAgentGrpcServer(dispatchers, maxBlobSizeInBytes));

        // default max message size in grpc is 4MB. if our maxBlobSize is greater than 4MB then we should configure this
        // limit in the netty based grpc server.
        if (maxBlobSizeInBytes > 4 * 1024 * 1024) {
            builder.maxMessageSize(maxBlobSizeInBytes);
        }

        server = builder.build().start();

        LOGGER.info("blob agent grpc server started on port {}....", port);

        try {
            server.awaitTermination();
        } catch (InterruptedException ex) {
            LOGGER.error("blob agent server has been interrupted with exception", ex);
        }
    }

    @VisibleForTesting
    List<BlobDispatcher> loadAndInitializeDispatchers(Config config, ClassLoader cl) {
        List<BlobDispatcher> dispatchers = new ArrayList<>();
        final ServiceLoader<BlobDispatcher> loadedDispatchers = ServiceLoader.load(BlobDispatcher.class, cl);
        final Config dispatchersConfig = config.getConfig(DISPATCHERS);

        for (final BlobDispatcher blobDispatcher : loadedDispatchers) {
            final Config currentDispatcherConfig = dispatchersConfig.getConfig(blobDispatcher.getName());

            blobDispatcher.initialize(currentDispatcherConfig);

            dispatchers.add(blobDispatcher);
        }

        Validate.notEmpty(dispatchers, "Blob agent dispatchers can't be an empty set");

        return dispatchers;
    }

    @Override
    public void close() {
        try {
            for (final BlobDispatcher dispatcher : dispatchers) {
                dispatcher.close();
            }
            LOGGER.info("shutting down gRPC server and jmx reporter");
            server.shutdown();
        } catch (Exception ignored) {
        }
    }
}
