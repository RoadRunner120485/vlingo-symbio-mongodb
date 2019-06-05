package io.vlingo.symbio.store.mongodb.journal.reader;

import com.mongodb.client.MongoCollection;
import io.vlingo.symbio.store.mongodb.journal.JournalDocumentAdapter;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.bson.Document;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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
    public List<ResolvedGap> resolveGaps(DetectedGapsProvider token, int maxEntries) {
        final List<ResolvedGap> resolvedGaps = new ArrayList<>();
        int entries = 0;

        token.getDetectedGaps()
                .forEach(offset -> knownGaps.add(new KnownGap(System.currentTimeMillis(), offset)));

        for (KnownGap knownGap : knownGaps) {
            final SequenceOffset offset = knownGap.gap;

            final Document document = journal.find(JournalDocumentAdapter.queryFor(offset)).first();
            if (document != null) {
                final JournalDocumentAdapter entry = new JournalDocumentAdapter(document);
                resolvedGaps.add(ResolvedGap.<Document>at(offset).with(entry));
                entries += entry.getEntries().size();

                if (entries >= maxEntries) {
                    break;
                }
            } else if (knownGap.registrationTime + unit.toMillis(gapTimeout) < System.currentTimeMillis()) {
                resolvedGaps.add(ResolvedGap.<Document>at(offset).withoutData());
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
