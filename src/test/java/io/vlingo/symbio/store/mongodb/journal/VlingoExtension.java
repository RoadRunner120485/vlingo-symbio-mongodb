package io.vlingo.symbio.store.mongodb.journal;

import io.vlingo.actors.World;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

public class VlingoExtension implements BeforeAllCallback, ParameterResolver {

    private static final ExtensionContext.Namespace VLINGO_EXTENSION_NAMESPACE = ExtensionContext.Namespace.create(VlingoExtension.class.getSimpleName() + "_namespace_key");
    private static final String WORLD_STORE_NAME = VlingoExtension.class.getSimpleName() + "_world";

    @Override
    public void beforeAll(ExtensionContext extensionContext) throws Exception {
        final ExtensionContext.Store store = extensionContext.getStore(VLINGO_EXTENSION_NAMESPACE);

        store.getOrComputeIfAbsent(WORLD_STORE_NAME, name -> new ClosableWorld(World.startWithDefaults(name)), ClosableWorld.class);
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        return parameterContext.getParameter().getType()
                .equals(World.class);
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        return extensionContext.getStore(VLINGO_EXTENSION_NAMESPACE).get(WORLD_STORE_NAME, ClosableWorld.class).world;
    }

    private class ClosableWorld implements ExtensionContext.Store.CloseableResource {
        final World world;

        private ClosableWorld(World world) {
            this.world = world;
        }

        @Override
        public void close() throws Throwable {
            world.stage().stop();
        }
    }
}
