package io.vlingo.symbio.store.mongodb.journal.reader;

import io.vlingo.symbio.store.mongodb.journal.JournalDocumentAdapter;
import lombok.Value;

import java.util.Optional;

/**
 * A {@link ResolvedGap} is a detected gap whose corresponding journal entry has been found now or that has been evicted
 * by the {@link GapResolver} because one could assume, that the entry has never been written to the
 * {@link io.vlingo.symbio.store.journal.Journal} e.g. because of an error.
 */
@Value
public class ResolvedGap {

    /**
     * The {@link SequenceOffset} for which no {@Link io.vlingo.symbio.Entry} was found in a first place
     *
     * @return the offset
     */
    private final SequenceOffset offset;

    private final JournalDocumentAdapter data;

    /**
     * The raw data of the resolved {@link io.vlingo.symbio.Entry} if it was found.
     *
     * @return the raw data belonging to {@link ResolvedGap#getOffset()}
     */
    public Optional<JournalDocumentAdapter> getData() {
        return Optional.ofNullable(data);
    }

    public boolean hasData() {
        return data != null;
    }

    private ResolvedGap(SequenceOffset offset, JournalDocumentAdapter data) {
        this.offset = offset;
        this.data = data;
    }

    public static ResolvedGap.Builder at(SequenceOffset offset) {
        final ResolvedGap.Builder result = new ResolvedGap.Builder();
        result.offset = offset;
        return result;
    }

    public static class Builder<T> {
        private SequenceOffset offset;


        public ResolvedGap withoutData() {
            return new ResolvedGap(offset, null);
        }

        public ResolvedGap with(JournalDocumentAdapter data) {
            return new ResolvedGap(offset, data);
        }
    }

}