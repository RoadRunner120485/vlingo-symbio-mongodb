package io.vlingo.symbio.store.mongodb.journal.reader;

import java.util.Set;

/**
 * The {@link DetectedGapsProvider} is aware of gaps that has been detected while reading a journal. Gaps could happen
 * because of entries that were out-of-order, delayed or have not beed written due to errors.
 */
public interface DetectedGapsProvider {

    Set<SequenceOffset> getDetectedGaps();

}
