package io.vlingo.symbio.store.mongodb.journal.reader;

import io.vlingo.lattice.model.sourcing.EventSourced;
import lombok.NonNull;

import java.util.UUID;

class CounterActor extends EventSourced implements Counter {

    private final UUID id;
    private Integer counter;

    static {
        EventSourced.registerConsumer(
                CounterActor.class,
                CounterIncremented.class,
                CounterActor::when);
        EventSourced.registerConsumer(
                CounterActor.class,
                CounterDecremented.class,
                CounterActor::when);
        EventSourced.registerConsumer(
                CounterActor.class,
                CounterCreated.class,
                CounterActor::when);
    }

    public CounterActor(@NonNull UUID id) {
        this.id = id;
        this.counter = counter == null ? 0 : counter;
    }

    public void initialize(int start) {
        apply(new CounterCreated(start));
    }

    @Override
    public void increment() {
        apply(new CounterIncremented(1));
    }

    @Override
    public void decrement() {
        apply(new CounterDecremented(1));
    }

    void when(CounterCreated evt) {
        counter = evt.start();
    }

    void when(CounterIncremented evt) {
        counter = counter + evt.amount();
    }

    void when(CounterDecremented evt) {
        counter = counter - evt.amount();
    }

    @Override
    protected String streamName() {
        return id.toString();
    }

}
