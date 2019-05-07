package io.vlingo.symbio.store.mongodb.journal;

import com.google.gson.Gson;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;
import io.vlingo.actors.Actor;
import io.vlingo.actors.Address;
import io.vlingo.actors.Definition;
import io.vlingo.common.Completes;
import io.vlingo.common.Success;
import io.vlingo.common.Tuple2;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.EntryAdapter;
import io.vlingo.symbio.Source;
import io.vlingo.symbio.State;
import io.vlingo.symbio.StateAdapter;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.journal.Journal;
import io.vlingo.symbio.store.journal.JournalListener;
import io.vlingo.symbio.store.journal.JournalReader;
import io.vlingo.symbio.store.journal.StreamReader;
import io.vlingo.symbio.store.mongodb.Configuration;
import io.vlingo.symbio.store.mongodb.journal.reader.MongoDBJournalReaderActor;
import io.vlingo.symbio.store.mongodb.journal.reader.MongoDBStreamReaderActor;
import io.vlingo.symbio.store.mongodb.journal.reader.SequenceOffset;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.time.Clock;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class MongoDBJournalActor extends Actor implements Journal<Document> {

    public static final String JOURNAL_COLLECTION_NAME = "symbio_journal";

    private final Clock clock = Clock.systemUTC();

    private SequenceOffset sequencePointer;

    private final Configuration configuration;
    private final MongoCollection<Document> journal;

    private final Map<String, JournalReader<DocumentEntry>> journalReaders;
    private final Map<String, StreamReader<Document>> streamReaders;
    private final Map<Class<?>, EntryAdapter<Source<?>, Entry<Document>>> entryAdapters;
    private final Map<Class<?>, StateAdapter<?, State<Document>>> stateAdapters;

    private final JournalListener<Document> journalListener;

    private final Gson gson = new Gson();

    public MongoDBJournalActor(JournalListener<Document> journalListener, Configuration configuration) {
        this.configuration = configuration;

        final MongoDatabase database = configuration.client().getDatabase(configuration.datebaseName());
        journal = database.getCollection(JOURNAL_COLLECTION_NAME).withWriteConcern(configuration.writeConcern());

        this.journalListener = journalListener;

        this.journalReaders = new HashMap<>();
        this.streamReaders = new HashMap<>();
        this.entryAdapters = new HashMap<>();
        this.stateAdapters = new HashMap<>();

        initSequenceNumber(configuration.getJournalId());
    }

    private void initSequenceNumber(String journalId) {
        final Document result = journal.find(new Document("sequence.id", journalId)).sort(new Document("sequence.offset", -1)).first();
        if (result == null) {
            this.sequencePointer = new SequenceOffset(journalId);
        } else {
            long latestSequenceNumber = result.get("sequence", Document.class).getLong("offset");
            this.sequencePointer = new SequenceOffset(journalId, latestSequenceNumber).next();
        }
    }

    public <S, ST> void append(String streamName, int fromStreamVersion, Source<S> source, AppendResultInterest interest, Object o) {
        final Tuple2<List<Entry<Document>>, State<Document>> result = appendAllWithInternal(streamName, fromStreamVersion, Collections.singletonList(source), null);
        interest.appendResultedIn(Success.of(Result.Success), streamName, fromStreamVersion, source, Optional.empty(), o);
        journalListener.appended(result._1.get(0));
    }

    public <S, ST> void appendWith(String streamName, int fromStreamVersion, Source<S> source, ST snapshot, AppendResultInterest interest, Object o) {
        final Tuple2<List<Entry<Document>>, State<Document>> result = appendAllWithInternal(streamName, fromStreamVersion, Collections.singletonList(source), snapshot);
        interest.appendResultedIn(Success.of(Result.Success), streamName, fromStreamVersion, source, Optional.ofNullable(snapshot), o);
        journalListener.appendedWith(result._1.get(0), result._2);
    }

    public <S, ST> void appendAll(String streamName, int fromStreamVersion, List<Source<S>> sources, AppendResultInterest interest, Object o) {
        final Tuple2<List<Entry<Document>>, State<Document>> result = appendAllWithInternal(streamName, fromStreamVersion, sources, null);
        interest.appendAllResultedIn(Success.of(Result.Success), streamName, fromStreamVersion, sources, Optional.empty(), o);
        journalListener.appendedAll(result._1);
    }

    public <S, ST> void appendAllWith(String streamName, int fromStreamVersion, List<Source<S>> sources, ST snapshot, AppendResultInterest interest, Object o) {
        final Tuple2<List<Entry<Document>>, State<Document>> result = appendAllWithInternal(streamName, fromStreamVersion, sources, snapshot);
        interest.appendAllResultedIn(Success.of(Result.Success), streamName, fromStreamVersion, sources, Optional.ofNullable(snapshot), o);
        journalListener.appendedAllWith(result._1, result._2);
    }

    private <S, ST> Tuple2<List<Entry<Document>>, State<Document>> appendAllWithInternal(String streamName, int fromStreamVersion, List<Source<S>> sources, ST snapshot) {
        final AtomicInteger version = new AtomicInteger(fromStreamVersion);

        final List<Entry<Document>> entries = sources.stream()
                .map(this::asEntry)
                .collect(Collectors.toList());

        final List<Document> documents = entries.stream()
                .map(entry -> entryAsDocument(entry, version.getAndIncrement()))
                .sorted(Comparator.comparingInt(doc -> doc.getInteger("streamVersion")))
                .collect(Collectors.toList());

        final int maxVersion = version.get() - 1;

        final Document combinedDocument = new Document("streamName", streamName)
                .append("sequence", new Document("timestamp", clock.millis()).append("id", sequencePointer.getId()).append("offset", sequencePointer.getOffset()))
                .append("streamVersionStart", fromStreamVersion)
                .append("streamVersionEnd", maxVersion)
                .append("entries", documents)
                .append("hasSnapshot", snapshot != null);

        State<Document> state = null;
        if (snapshot != null) {
            state = asState(fromStreamVersion, snapshot);
            combinedDocument
                    .append("state", new Document("document", state.data)
                            .append("type", state.type)
                            .append("dataVersion", state.dataVersion)
                            .append("typeVersion", state.typeVersion)
                            .append("metadata", asJson(state.metadata))
                    );
        }

        final Document filter = new Document("streamName", streamName)
                .append("streamVersionEnd", new Document("$gte", fromStreamVersion));
        final Document update = new Document("$setOnInsert", combinedDocument);

        final UpdateResult updateResult = journal.updateOne(filter, update, new UpdateOptions().upsert(true));

        boolean success = updateResult.getUpsertedId() != null;

        if (success) {
            this.sequencePointer = sequencePointer.next();
        } else {
            // TODO ERROR HANDLING!!!
            throw new IllegalStateException("");
        }

        return Tuple2.from(entries, state);
    }

    private String asJson(Object source) {
        return gson.toJson(source);
    }

    private Document entryAsDocument(Entry<Document> entry, int streamVersion) {
        return new Document("_id", new ObjectId())
                .append("streamVersion", streamVersion)
                .append("typeVersion", entry.typeVersion())
                .append("metadata", asJson(entry.metadata()))
                .append("document", entry.entryData())
                .append("type", entry.type());
    }

    @SuppressWarnings("unchecked")
    public Completes<JournalReader<DocumentEntry>> journalReader(String name) {
        final JournalReader<DocumentEntry> reader = journalReaders.computeIfAbsent(name, (key) -> {
            Address address = stage().world().addressFactory().uniquePrefixedWith("eventJournalReader-" + name);
            return stage().actorFor(
                    JournalReader.class,
                    Definition.has(
                            MongoDBJournalReaderActor.class,
                            Definition.parameters(name, this.configuration)
                    ),
                    address
            );
        });

        return completes().with(reader);
    }

    @SuppressWarnings("unchecked")
    public Completes<StreamReader<Document>> streamReader(String name) {
        final StreamReader<Document> reader = streamReaders.computeIfAbsent(name, (key) -> {
            Address address = stage().world().addressFactory().uniquePrefixedWith("eventStreamReader-" + key);
            return stage().actorFor(
                    StreamReader.class,
                    Definition.has(
                            MongoDBStreamReaderActor.class,
                            Definition.parameters(name, this.configuration)
                    ),
                    address
            );
        });


        return completes().with(reader);
    }

//    @Override
//    public <S extends Source<?>, E extends Entry<?>> void registerEntryAdapter(Class<S> aClass, EntryAdapter<S, E> entryAdapter) {
////        throw new UnsupportedOperationException("Register Adapters in SourcedTypeRegistry only");
//        entryAdapters.put(aClass, (EntryAdapter<Source<?>, Entry<Document>>) entryAdapter);
//
//    }
//
//    @Override
//    public <S, R extends State<?>> void registerStateAdapter(Class<S> aClass, StateAdapter<S, R> stateAdapter) {
////        throw new UnsupportedOperationException("Register Adapters in SourcedTypeRegistry only");
//        stateAdapters.put(aClass, (StateAdapter<?, State<Document>>) stateAdapter);
//    }

    private Entry<Document> asEntry(Source<?> source) {
//        final SourcedTypeRegistry sourcedTypeRegistry = getSourcedTypeRegistry();
//        final Entry<?> entry = sourcedTypeRegistry.info(SOURCED_CLASS_MISSING).entryAdapterProvider.asEntry(source);
//        if (!entryAdapters.containsKey(source.getClass())) {
//            throw new IllegalStateException(String.format("No EntryAdapter for source '%s' found", source.getClass()));
//        }

        final Entry<?> entry = new DefaultDocumentEntryAdapter<>().toEntry(source);//)entryAdapters.get(source.getClass()).toEntry(source);

        if (!(entry.entryData() instanceof Document)) {
            throw new IllegalStateException(String.format("entryData must be from type '%s' but was '%s'", Document.class, entry.entryData().getClass()));
        }

        return (Entry<Document>) entry;
    }

    private State<Document> asState(int streamVersion, Object snapshot) {
//        final SourcedTypeRegistry sourcedTypeRegistry = getSourcedTypeRegistry();
//        final State<?> state = sourcedTypeRegistry.info(SOURCED_CLASS_MISSING).stateAdapterProvider().asRaw(snapshot, streamVersion);

//        if (!stateAdapters.containsKey(snapshot.getClass())) {
//            throw new IllegalStateException(String.format("No StateAdapter for source '%s' found", snapshot.getClass()));
//        }

//        final StateAdapter<Object, State<Document>> stateAdapter = (StateAdapter<Object, State<Document>>) stateAdapters.get(snapshot.getClass());
        final State<?> state = new DefaultDocumentStateAdapter<>().toRawState(snapshot, streamVersion);

        if (!(state.data instanceof Document)) {
            throw new IllegalStateException(String.format("data  must be from type '%s' but was '%s'", Document.class, state.data.getClass()));
        }

        return (State<Document>) state;
    }

}
