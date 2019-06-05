package io.vlingo.symbio.store.mongodb.journal.reader;

import io.vlingo.actors.Definition;
import io.vlingo.actors.Stage;

import java.util.UUID;

interface Counter {

    static Counter create(Stage stage, int initialValue) {
        final Counter counterActor = stage.actorFor(Counter.class, Definition.has(CounterActor.class, Definition.parameters(UUID.randomUUID(), initialValue)));
        counterActor.initialize(initialValue);
        return counterActor;
    }

    void initialize(int start);

    void increment();

    void decrement();
}
