package io.vlingo.symbio.store.mongodb.journal;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.vlingo.common.Completes;
import io.vlingo.common.Tuple2;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.State;
import io.vlingo.symbio.store.journal.Stream;
import io.vlingo.symbio.store.journal.StreamReader;
import io.vlingo.symbio.store.mongodb.Configuration;
import org.bson.Document;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class MongoDBStreamReaderActor extends AbstractMongoJournalReadingActor implements StreamReader<Document> {

    private final String name;
    private final MongoCollection<Document> journal;

    public MongoDBStreamReaderActor(String name, Configuration configuration) {
        this.name = name;
        final MongoDatabase database = configuration.client().getDatabase(configuration.datebaseName());
        this.journal = database.getCollection(MongoDBJournalActor.JOURNAL_COLLECTION_NAME);
    }

    public Completes<Stream<Document>> streamFor(String streamName) {
        final Document snapshotDocument = journal.find(new Document("streamName", streamName).append("hasSnapshot", true))
                .sort(new Document("streamVersionEnd", -1))
                .limit(1)
                .first();

        final Optional<State<Document>> state = Optional.ofNullable(snapshotDocument).map(d -> asState(streamName, d));
        final Integer streamVersion = state.map(s -> s.dataVersion).orElse(1);
        final List<Entry<Document>> entries = entriesFromVersion(streamName, streamVersion);

        if (entries.isEmpty() && !state.isPresent()) {
            return completes().with(unknownStream(streamName));
        } else {
            return completes().with(new Stream<>(streamName, streamVersion + entries.size(), entries, state.orElse(DocumentState.Null)));
        }
    }

    private Stream<Document> unknownStream(String streamName) {
        return new Stream<>(streamName, 0, Collections.emptyList(), DocumentState.Null);
    }

    public Completes<Stream<Document>> streamFor(String streamName, int streamVersion) {
        final List<Entry<Document>> entries = entriesFromVersion(streamName, streamVersion);


        return completes().with(new Stream<>(streamName, streamVersion + entries.size(), entries, DocumentState.Null));
    }

    private List<Entry<Document>> entriesFromVersion(String streamName, int streamVersion) {
        final FindIterable<Document> documents = journal.find(new Document("streamName", streamName).append("streamVersionEnd", new Document("$gte", streamVersion))).sort(new Document("streamVersionEnd", 1));

        return StreamSupport.stream(documents.spliterator(), false)
                .flatMap(document -> document.getList("entries", Document.class).stream())
                .filter(entry -> entry.getInteger("streamVersion") >= streamVersion)
                .map(entry -> Tuple2.from(entry.getInteger("streamVersion"), asEntry(entry)))
                .sorted(Comparator.comparingInt(tuple -> tuple._1))
                .map(t -> t._2)
                .collect(Collectors.toList());
    }

}
