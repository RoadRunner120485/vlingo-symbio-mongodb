package io.vlingo.symbio.store.mongodb.journal.reader;

import com.mongodb.client.MongoCollection;
import io.vlingo.common.Tuple2;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.bson.Document;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class TimeoutGapResolver implements GapResolver {

    private final MongoCollection<Document> journal;
    private final long gapTimeout;
    private final TimeUnit unit;
    private Set<KnownGap> knownGaps = new HashSet<>();

    public TimeoutGapResolver(MongoCollection<Document> journal, long gapTimeout, TimeUnit unit) {
        this.journal = journal;
        this.gapTimeout = gapTimeout;
        this.unit = unit;
    }

    @Override
    public List<Tuple2<SequenceOffset, Optional<Document>>> resolveGaps(CursorToken token, int maxEntries) {
        final List<Tuple2<SequenceOffset, Optional<Document>>> resolvedGaps = new ArrayList<>();
        int entries = 0;

        token.getGaps()
                .forEach(offset -> knownGaps.add(new KnownGap(System.currentTimeMillis(), offset)));

        for (KnownGap knownGap : knownGaps) {
            final SequenceOffset pointer = knownGap.gap;
            final Document entry = journal.find(new Document("sequence.id", pointer.getId())
                    .append("sequence.offset", pointer.getOffset())).first();
            if (entry != null) {
                resolvedGaps.add(Tuple2.from(pointer, Optional.of(entry)));
                entries += entry.getList("entries", Document.class).size();

                if(entries >= maxEntries) {
                    break;
                }
            } else if (knownGap.registrationTime + unit.toMillis(gapTimeout) < System.currentTimeMillis()) {
                resolvedGaps.add(Tuple2.from(pointer, Optional.empty()));
            }
        }

        return resolvedGaps;
    }

    @Value
    @EqualsAndHashCode(of = "gap")
    private static class KnownGap {
        private long registrationTime;
        private SequenceOffset gap;
    }
}
