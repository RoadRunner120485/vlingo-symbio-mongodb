package io.vlingo.symbio.store.mongodb.journal.reader;

import java.util.List;

/**
 * A {@link GapResolver} resolves detected when the corresponding {@link io.vlingo.symbio.Entry} was found in Journal
 * or just marks it as resolved if one could safely assume that a corresponding {@link io.vlingo.symbio.Entry} has never
 * been written to Journal.
 */
public interface GapResolver {

    List<ResolvedGap> resolveGaps(DetectedGapsProvider provider, int maxEntries);

}
