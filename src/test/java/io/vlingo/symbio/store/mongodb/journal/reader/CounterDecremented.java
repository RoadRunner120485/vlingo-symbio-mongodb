package io.vlingo.symbio.store.mongodb.journal.reader;

import io.vlingo.lattice.model.DomainEvent;

public class CounterDecremented extends DomainEvent {
    private final int amount;

    public CounterDecremented(int amount) {
        this.amount = amount;
    }

    public int amount() {
        return amount;
    }
}
