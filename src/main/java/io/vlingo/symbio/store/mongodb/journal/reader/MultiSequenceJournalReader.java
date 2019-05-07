package io.vlingo.symbio.store.mongodb.journal.reader;

import io.vlingo.common.Completes;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.store.journal.JournalReader;

import java.util.Set;

public interface MultiSequenceJournalReader<T> extends JournalReader<Entry<T>> {

    Completes<Set<SequenceOffset>> seekTo(SequenceOffset... offsets);
}
