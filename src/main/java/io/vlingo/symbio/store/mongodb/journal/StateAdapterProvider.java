package io.vlingo.symbio.store.mongodb.journal;

import io.vlingo.actors.World;

import java.util.UUID;

public class StateAdapterProvider {
    static final String SERVICE_NAME = UUID.randomUUID().toString();

    public static StateAdapterProvider instance(World world) {
        world.resolveDynamic(SERVICE_NAME, StateAdapterProvider.class);
        return null;
    }

}
