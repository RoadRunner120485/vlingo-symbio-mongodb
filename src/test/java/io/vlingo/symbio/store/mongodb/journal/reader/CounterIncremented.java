package io.vlingo.symbio.store.mongodb.journal.reader;

import io.vlingo.lattice.model.DomainEvent;

public class CounterIncremented extends DomainEvent {
    private final int amount;

    public CounterIncremented(int amount) {
        this.amount = amount;
    }

    public int amount() {
        return amount;
    }
}
