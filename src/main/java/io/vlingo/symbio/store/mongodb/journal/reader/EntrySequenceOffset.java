package io.vlingo.symbio.store.mongodb.journal.reader;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.Value;
import lombok.experimental.NonFinal;

import java.util.Set;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toSet;

@Value
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@NonFinal
public class EntrySequenceOffset extends SequenceOffset {

    private final int entriesSize;

    private final int entryIndex;

    public EntrySequenceOffset(String id) {
        this(id, null);
    }

    public EntrySequenceOffset(String id, Long offset) {
        this(id, offset, null);
    }

    public EntrySequenceOffset(String id, Long offset, Integer entryIndex) {
        this(id, offset, entryIndex, null);
    }

    public EntrySequenceOffset(SequenceOffset sequenceOffset, Integer entryIndex, Integer entriesSize) {
        this(sequenceOffset.getId(), sequenceOffset.getOffset(), entryIndex, entriesSize);
    }

    public EntrySequenceOffset(String id, Long offset, Integer entryIndex, Integer entriesSize) {
        super(id, offset);
        this.entryIndex = entryIndex == null ? 0 : entryIndex;
        this.entriesSize = entriesSize == null ? 1 : entriesSize;
    }


    public boolean hasNext() {
        return entryIndex + 1 < entriesSize;
    }

    public EntrySequenceOffset nextEntry() {
        if (hasNext()) {
            return new EntrySequenceOffset(getId(), getOffset(), entryIndex + 1, entriesSize);
        } else {
            return null;
        }
    }

    public SequenceOffset asOffset() {
        return new SequenceOffset(getId(), getOffset());
    }

    public Set<EntrySequenceOffset> allSiblings() {
        return IntStream.range(0, entriesSize)
                .boxed()
                .map(idx -> new EntrySequenceOffset(getId(), getOffset(), idx, getEntriesSize()))
                .filter(sbl -> sbl.entryIndex != this.entryIndex)
                .collect(toSet());
    }

}
