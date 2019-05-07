package io.vlingo.symbio.store.mongodb.journal.reader;

import lombok.Builder;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;
import lombok.experimental.Wither;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

@Value
public class CursorToken {

    private final SequenceOffset currentDocumentOffset;
    @Wither
    private final int currentDocumentIndex;
    private final int currentDocumentSize;
    private final Map<String, SequenceOffset> topOfSequencePointers;
    private final Set<SequenceOffset> gaps;

    @Builder(toBuilder = true)
    private CursorToken(SequenceOffset currentDocumentOffset, int currentDocumentIndex, int currentDocumentSize, @NonNull @Singular Map<String, SequenceOffset> topOfSequencePointers, @Singular Set<SequenceOffset> gaps) {
        this.currentDocumentOffset = currentDocumentOffset;
        this.currentDocumentIndex = currentDocumentIndex;
        this.currentDocumentSize = currentDocumentSize;
        this.topOfSequencePointers = Collections.unmodifiableMap(topOfSequencePointers);
        this.gaps = gaps == null ? Collections.emptySet() : Collections.unmodifiableSet(gaps);
    }

    public static CursorToken withSequenceOffsets(SequenceOffset... offsets) {
        return new CursorToken(null, 0, 0, Arrays.stream(offsets).collect(Collectors.toMap(SequenceOffset::getId, Function.identity())), Collections.emptySet());
    }

    public static CursorToken beginning() {
        return new CursorToken(null, 0, 0, Collections.emptyMap(), Collections.emptySet());
    }

    public static CursorToken init(SequenceOffset currentDocumentOffset, int currentDocumentIndex, int currentDocumentSize) {
        return CursorToken.builder()
                .currentDocumentOffset(currentDocumentOffset)
                .currentDocumentIndex(currentDocumentIndex)
                .currentDocumentSize(currentDocumentSize)
                .topOfSequencePointer(currentDocumentOffset.getId(), currentDocumentOffset)
                .build();
    }

    public boolean isBeginning() {
        return currentDocumentOffset == null;
    }

    public boolean hasCurrentDocumentMoreEntries() {
        return currentDocumentIndex < currentDocumentSize;
    }

    public CursorToken nextIndex() {
        if (this.hasCurrentDocumentMoreEntries()) {
            return this.toBuilder().currentDocumentIndex(currentDocumentIndex + 1).build();
        } else {
            return this;
        }
    }

    public Set<SequenceOffset> findGaps(SequenceOffset nextOffset) {
        final SequenceOffset knownTopOfSequence = topOfSequencePointers.get(nextOffset.getId());
        final long nextExpectedSequenceNumber = knownTopOfSequence == null ? 1L : knownTopOfSequence.next().getOffset();

        final boolean hasGaps = nextExpectedSequenceNumber < nextOffset.getOffset();

        if (hasGaps) {
            return LongStream.range(nextExpectedSequenceNumber, nextOffset.getOffset())
                    .mapToObj(nextOffset::seekTo)
                    .collect(Collectors.toSet());
        } else {
            return Collections.emptySet();
        }
    }

    public CursorToken withCurrentDocumentOffset(SequenceOffset documentOffset, int documentIndex, int documentSize) {
        return this.toBuilder()
                .currentDocumentOffset(documentOffset)
                .currentDocumentIndex(documentIndex)
                .currentDocumentSize(documentSize)
                .topOfSequencePointer(documentOffset.getId(), documentOffset)
                .build();
    }

    public CursorToken addGaps(Set<SequenceOffset> gaps) {
        if (gaps.isEmpty()) {
            return this;
        } else {

            final Set<SequenceOffset> newGaps = new HashSet<>(this.gaps.size() + gaps.size());
            newGaps.addAll(this.gaps);
            newGaps.addAll(gaps);

            return this.toBuilder()
                    .gaps(newGaps)
                    .build();
        }
    }

    public CursorToken removeGaps(Set<SequenceOffset> gaps) {
        if (gaps.isEmpty()) {
            return this;
        } else {

            final Set<SequenceOffset> newGaps = this.gaps.stream()
                    .filter(offset -> !gaps.contains(offset))
                    .collect(Collectors.toSet());

            return this.toBuilder()
                    .gaps(newGaps)
                    .build();
        }
    }

    public void forEachSequence(BiConsumer<SequenceOffset, Boolean> consumer) {
        topOfSequencePointers.values().forEach(pointer -> consumer.accept(pointer, currentDocumentOffset != null && pointer.getId().equals(currentDocumentOffset.getId())));
    }

}
