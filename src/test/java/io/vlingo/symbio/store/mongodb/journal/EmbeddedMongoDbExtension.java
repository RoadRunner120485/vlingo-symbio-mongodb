package io.vlingo.symbio.store.mongodb.journal;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

import java.io.IOException;
import java.net.ServerSocket;

public class EmbeddedMongoDbExtension implements BeforeAllCallback, ParameterResolver {

    private static final ExtensionContext.Namespace MONGO_EXTENSION_NAMESPACE = ExtensionContext.Namespace.create(EmbeddedMongoDbExtension.class.getSimpleName() + "_namespace_key");
    private static final String RESOURCES_STORE_NAME = EmbeddedMongoDbExtension.class.getSimpleName() + "_world";

    @Override
    public void beforeAll(ExtensionContext extensionContext) throws Exception {
        final ExtensionContext.Store store = extensionContext.getStore(MONGO_EXTENSION_NAMESPACE);

        store.getOrComputeIfAbsent(RESOURCES_STORE_NAME, name -> {
            final MongodStarter starter = MongodStarter.getDefaultInstance();

            try {
                final ServerSocket s = new ServerSocket(0);
                final Net net = new Net("localhost", s.getLocalPort(), Network.localhostIsIPv6());
                final MongodExecutable mongodExe = starter.prepare(new MongodConfigBuilder()
                        .version(Version.Main.PRODUCTION)
                        .net(net)
                        .build());
                final MongodProcess mongod = mongodExe.start();
                final MongoClient mongo = MongoClients.create("mongodb://localhost:" + net.getPort());
                return new ClosableMongoResources(mongodExe, mongod, mongo);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }, ClosableMongoResources.class);
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        return parameterContext.getParameter().getType()
                .equals(MongoClient.class);
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        final ExtensionContext.Store store = extensionContext.getStore(MONGO_EXTENSION_NAMESPACE);
        return store.get(RESOURCES_STORE_NAME, ClosableMongoResources.class).mongo;
    }

    private class ClosableMongoResources implements ExtensionContext.Store.CloseableResource {
        private final MongodExecutable mongodExe;
        private final MongodProcess mongod;
        private final MongoClient mongo;

        private ClosableMongoResources(MongodExecutable mongodExe, MongodProcess mongod, MongoClient mongo) {
            this.mongodExe = mongodExe;
            this.mongod = mongod;
            this.mongo = mongo;
        }

        @Override
        public void close() throws Throwable {
            mongo.close();
            mongod.stop();
            mongodExe.stop();
        }
    }
}
