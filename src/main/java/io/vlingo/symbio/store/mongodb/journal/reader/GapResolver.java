package io.vlingo.symbio.store.mongodb.journal.reader;

import io.vlingo.common.Tuple2;
import org.bson.Document;

import java.util.List;
import java.util.Optional;

public interface GapResolver {

    List<Tuple2<SequenceOffset, Optional<Document>>> resolveGaps(CursorToken token, int maxEntries);
}
