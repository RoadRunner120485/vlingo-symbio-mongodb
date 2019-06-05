package io.vlingo.symbio.store.mongodb.journal.reader;

import lombok.Builder;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;
import lombok.experimental.Wither;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static java.util.stream.Collectors.toSet;

@Value
public class CursorToken implements DetectedGapsProvider {

    private final SequenceOffset currentDocumentOffset;
    @Wither
    private final int currentDocumentIndex;
    private final int currentDocumentSize;
    private final Map<String, SequenceOffset> sequenceHeads;
    private final Set<SequenceOffset> detectedGaps;

    @Builder(toBuilder = true)
    private CursorToken(SequenceOffset currentDocumentOffset, int currentDocumentIndex, int currentDocumentSize, @NonNull @Singular Map<String, SequenceOffset> sequenceHeads, @Singular Set<SequenceOffset> detectedGaps) {
        this.currentDocumentOffset = currentDocumentOffset;
        this.currentDocumentIndex = currentDocumentIndex;
        this.currentDocumentSize = currentDocumentSize;
        this.sequenceHeads = Collections.unmodifiableMap(sequenceHeads);
        this.detectedGaps = detectedGaps == null ? Collections.emptySet() : Collections.unmodifiableSet(detectedGaps);
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
                .sequenceHead(currentDocumentOffset.getId(), currentDocumentOffset)
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
        final SequenceOffset knownTopOfSequence = sequenceHeads.get(nextOffset.getId());
        final long nextExpectedSequenceNumber = knownTopOfSequence == null ? 1L : knownTopOfSequence.next().getOffset();

        final boolean hasGaps = nextExpectedSequenceNumber < nextOffset.getOffset();

        if (hasGaps) {
            return LongStream.range(nextExpectedSequenceNumber, nextOffset.getOffset())
                    .mapToObj(nextOffset::seekTo)
                    .collect(toSet());
        } else {
            return Collections.emptySet();
        }
    }

    public CursorToken withCurrentDocumentOffset(SequenceOffset documentOffset, int documentIndex, int documentSize) {
        return this.toBuilder()
                .currentDocumentOffset(documentOffset)
                .currentDocumentIndex(documentIndex)
                .currentDocumentSize(documentSize)
                .sequenceHead(documentOffset.getId(), documentOffset)
                .build();
    }

    public CursorToken detected(Set<SequenceOffset> gaps) {
        if (gaps.isEmpty()) {
            return this;
        } else {

            final Set<SequenceOffset> newGaps = new HashSet<>(this.detectedGaps.size() + gaps.size());
            newGaps.addAll(this.detectedGaps);
            newGaps.addAll(gaps);

            return this.toBuilder()
                    .detectedGaps(newGaps)
                    .build();
        }
    }

    public CursorToken resolved(List<ResolvedGap> gaps) {
        if (gaps.isEmpty()) {
            return this;
        } else {
            final Set<SequenceOffset> resolvedGapOffsets = gaps.stream()
                    .map(ResolvedGap::getOffset)
                    .collect(toSet());

            final Set<SequenceOffset> newGaps = this.detectedGaps.stream()
                    .filter(offset -> !resolvedGapOffsets.contains(offset))
                    .collect(toSet());

            return this.toBuilder()
                    .detectedGaps(newGaps)
                    .build();
        }
    }

    public void forEachSequenceHead(BiConsumer<SequenceOffset, Boolean> consumer) {
        sequenceHeads.values().forEach(pointer -> consumer.accept(pointer, currentDocumentOffset != null && pointer.getId().equals(currentDocumentOffset.getId())));
    }

}
