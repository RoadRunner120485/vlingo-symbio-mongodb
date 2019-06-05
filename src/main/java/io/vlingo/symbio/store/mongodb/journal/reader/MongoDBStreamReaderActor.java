package io.vlingo.symbio.store.mongodb.journal.reader;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.vlingo.common.Completes;
import io.vlingo.symbio.BaseEntry;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.State;
import io.vlingo.symbio.store.journal.Stream;
import io.vlingo.symbio.store.journal.StreamReader;
import io.vlingo.symbio.store.mongodb.Configuration;
import io.vlingo.symbio.store.mongodb.journal.JournalDocumentAdapter;
import io.vlingo.symbio.store.mongodb.journal.JournalDocumentAdapter.JournalDocumentEntry;
import io.vlingo.symbio.store.mongodb.journal.MongoDBJournalActor;
import io.vlingo.symbio.store.mongodb.journal.adapter.DocumentState;
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
        final MongoDatabase database = configuration.client().getDatabase(configuration.databaseName());
        this.journal = database.getCollection(MongoDBJournalActor.JOURNAL_COLLECTION_NAME);
    }

    public Completes<Stream<Document>> streamFor(String streamName) {
        final Document snapshotDocument = journal.find(new Document("streamName", streamName).append("hasSnapshot", true))
                .sort(new Document("streamVersionEnd", -1))
                .limit(1)
                .first();

        final Optional<State<Document>> state = Optional.ofNullable(snapshotDocument)
                .map(JournalDocumentAdapter::new)
                .flatMap(d ->
                        d.getState()
                                .map(stateAdapter -> asState(streamName, stateAdapter))
                );

        final Integer streamVersion = state.map(s -> s.dataVersion).orElse(1);
        final List<Entry<Document>> entries = entriesFromVersion(streamName, streamVersion);

        if (entries.isEmpty() && !state.isPresent()) {
            return completes().with(unknownStream(streamName));
        } else {
            return completes().with(new Stream<>(streamName, streamVersion + entries.size(), cast(entries), state.orElse(DocumentState.Null)));
        }
    }

    private Stream<Document> unknownStream(String streamName) {
        return new Stream<>(streamName, 0, Collections.emptyList(), DocumentState.Null);
    }

    public Completes<Stream<Document>> streamFor(String streamName, int streamVersion) {
        final List<Entry<Document>> entries = entriesFromVersion(streamName, streamVersion);

        return completes().with(new Stream<>(streamName, streamVersion + entries.size(), cast(entries), DocumentState.Null));
    }

    @SuppressWarnings("unchecked")
    private List<BaseEntry<Document>> cast(List<? extends Entry<Document>> entries) {
        return (List<BaseEntry<Document>>) entries;
    }

    private List<Entry<Document>> entriesFromVersion(String streamName, int streamVersion) {
        final FindIterable<Document> documents = journal.find(new Document("streamName", streamName).append("streamVersionEnd", new Document("$gte", streamVersion))).sort(new Document("streamVersionEnd", 1));

        return StreamSupport.stream(documents.spliterator(), false)
                .map(JournalDocumentAdapter::new)
                .flatMap(document -> document.getEntries().stream())
                .filter(entry -> entry.getStreamVersion() >= streamVersion)
                .sorted(Comparator.comparingInt(JournalDocumentEntry::getStreamVersion))
                .map(this::asEntry)
                .collect(Collectors.toList());
    }

}
