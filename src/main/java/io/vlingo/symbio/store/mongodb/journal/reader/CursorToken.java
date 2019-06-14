package io.vlingo.symbio.store.mongodb.journal.reader;

import lombok.Builder;
import lombok.Singular;
import lombok.Value;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static java.util.stream.Collectors.toSet;

@Value
public class CursorToken implements DetectedGapsProvider {

    private final EntrySequenceOffset currentDocumentOffset;
    private final Map<String, SequenceOffset> sequenceHeads;
    private final Set<SequenceOffset> detectedGaps;
    private final Map<SequenceOffset, Set<EntrySequenceOffset>> openGapEntries;

    @Builder(toBuilder = true)
    private CursorToken(EntrySequenceOffset currentDocumentOffset, @Singular Map<String, SequenceOffset> sequenceHeads, @Singular Set<SequenceOffset> detectedGaps, @Singular Map<SequenceOffset, Set<EntrySequenceOffset>> openGapEntries) {
        this.currentDocumentOffset = currentDocumentOffset;
        this.sequenceHeads = sequenceHeads == null ? Collections.emptyMap() : Collections.unmodifiableMap(sequenceHeads);
        this.detectedGaps = detectedGaps == null ? Collections.emptySet() : Collections.unmodifiableSet(detectedGaps);
        this.openGapEntries = openGapEntries == null ? Collections.emptyMap() : Collections.unmodifiableMap(openGapEntries);
    }

    public static CursorToken withSequenceOffsets(SequenceOffset... offsets) {
        return new CursorToken(null, Arrays.stream(offsets).collect(Collectors.toMap(SequenceOffset::getId, Function.identity())), null, null);
    }

    public static CursorToken beginning() {
        return new CursorToken(null, null, null, null);
    }

    public static CursorToken init(SequenceOffset currentDocumentOffset, int currentDocumentIndex, int currentDocumentSize) {
        return CursorToken.builder()
                .currentDocumentOffset(new EntrySequenceOffset(currentDocumentOffset.getId(), currentDocumentOffset.getOffset(), currentDocumentIndex, currentDocumentSize))
                .sequenceHead(currentDocumentOffset.getId(), currentDocumentOffset)
                .build();
    }

    public boolean isBeginning() {
        return currentDocumentOffset == null;
    }

    public boolean hasCurrentDocumentMoreEntries() {
        return currentDocumentOffset != null && currentDocumentOffset.getEntryIndex() < currentDocumentOffset.getEntriesSize();
    }

    public CursorToken nextIndex() {
        if (this.hasCurrentDocumentMoreEntries()) {
            return this.toBuilder().currentDocumentOffset(currentDocumentOffset.nextEntry()).build();
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

    public CursorToken withCurrentDocumentOffset(EntrySequenceOffset currentDocumentOffset) {
        return this.toBuilder()
                .currentDocumentOffset(currentDocumentOffset)
                .sequenceHead(currentDocumentOffset.getId(), currentDocumentOffset.asOffset())
                .build();
    }

    public CursorToken withSequenceHead(SequenceOffset sequenceHead) {
        return this.toBuilder()
                .sequenceHead(sequenceHead.getId(), sequenceHead)
                .build();
    }

    public CursorToken detected(Set<SequenceOffset> gaps) {
        if (gaps.isEmpty()) {
            return this;
        } else {
            return this.toBuilder()
                    .detectedGaps(gaps)
                    .build();
        }
    }

    public CursorToken resolved(EntrySequenceOffset resolvedEntry) {
        final Map<SequenceOffset, Set<EntrySequenceOffset>> newOpenGapEntries = new HashMap<>(openGapEntries);
        final Set<SequenceOffset> newGaps = new HashSet<>(this.detectedGaps);

        final SequenceOffset key = resolvedEntry.asOffset();
        if (!newOpenGapEntries.containsKey(key)) {
            // append all missing sibling entries, but not the resolved one (as it obviously has been resolved)
            newOpenGapEntries.put(key, resolvedEntry.allSiblings());
        } else {
            // remove the resolved entry from the known missing entries
            final Set<EntrySequenceOffset> newOffsets = new HashSet<>(newOpenGapEntries.get(key));
            newOffsets.remove(resolvedEntry);
            newOpenGapEntries.replace(key, newOffsets);
        }

        // cleanup
        if (newOpenGapEntries.get(key).isEmpty()) {
            newOpenGapEntries.remove(key);
            newGaps.remove(key);
        }

        return this.toBuilder()
                .clearDetectedGaps()
                .clearOpenGapEntries()
                .openGapEntries(newOpenGapEntries)
                .detectedGaps(newGaps)
                .build();
    }

    public boolean isMissing(EntrySequenceOffset missing) {
        final SequenceOffset key = missing.asOffset();
        if (openGapEntries.containsKey(key)) {
            return openGapEntries.get(key).contains(missing);
        } else {
            return detectedGaps.contains(key);
        }
    }

    public void forEachSequenceHead(BiConsumer<SequenceOffset, Boolean> consumer) {
        sequenceHeads.values().forEach(pointer -> consumer.accept(pointer, currentDocumentOffset != null && pointer.getId().equals(currentDocumentOffset.getId())));
    }

}
