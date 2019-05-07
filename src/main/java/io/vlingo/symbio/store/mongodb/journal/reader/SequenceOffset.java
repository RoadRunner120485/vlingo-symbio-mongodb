package io.vlingo.symbio.store.mongodb.journal.reader;

import lombok.Value;
import lombok.experimental.NonFinal;

@Value
@NonFinal
public class SequenceOffset {

    public static final long START_OF_SEQUENCE = 1L;

    private String id;
    private long offset;

    public SequenceOffset(String id) {
        this(id, START_OF_SEQUENCE);
    }

    public SequenceOffset(String id, long offset) {
        this.id = id;
        this.offset = offset;
    }

    public SequenceOffset next() {
        return seekTo(offset + 1);
    }

    public SequenceOffset previous() {
        if (offset > START_OF_SEQUENCE) {
            return seekTo(offset - 1);
        } else {
            return this;
        }
    }

    public SequenceOffset seekTo(long newOffset) {
        return new SequenceOffset(id, newOffset);
    }

}
