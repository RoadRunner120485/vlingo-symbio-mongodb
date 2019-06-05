package io.vlingo.symbio.store.mongodb.journal.reader;

import io.vlingo.lattice.model.DomainEvent;

public class CounterCreated extends DomainEvent {
    private final int start;

    public CounterCreated(int start) {
        this.start = start;
    }

    public int start() {
        return start;
    }
}
