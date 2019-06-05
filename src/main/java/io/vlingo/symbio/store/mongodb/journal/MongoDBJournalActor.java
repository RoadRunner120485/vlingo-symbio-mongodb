package io.vlingo.symbio.store.mongodb.journal;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;
import io.vlingo.actors.Actor;
import io.vlingo.actors.Address;
import io.vlingo.actors.Definition;
import io.vlingo.common.Completes;
import io.vlingo.common.Failure;
import io.vlingo.common.Outcome;
import io.vlingo.common.Success;
import io.vlingo.common.Tuple2;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.Source;
import io.vlingo.symbio.State;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.StorageException;
import io.vlingo.symbio.store.journal.Journal;
import io.vlingo.symbio.store.journal.JournalListener;
import io.vlingo.symbio.store.journal.JournalReader;
import io.vlingo.symbio.store.journal.StreamReader;
import io.vlingo.symbio.store.mongodb.Configuration;
import io.vlingo.symbio.store.mongodb.journal.JournalDocumentAdapter.JournalDocumentEntry;
import io.vlingo.symbio.store.mongodb.journal.JournalDocumentAdapter.JournalDocumentSequence;
import io.vlingo.symbio.store.mongodb.journal.JournalDocumentAdapter.JournalDocumentState;
import io.vlingo.symbio.store.mongodb.journal.adapter.DefaultDocumentEntryAdapter;
import io.vlingo.symbio.store.mongodb.journal.adapter.DefaultDocumentStateAdapter;
import io.vlingo.symbio.store.mongodb.journal.adapter.DocumentEntry;
import io.vlingo.symbio.store.mongodb.journal.reader.MongoDBJournalReaderActor;
import io.vlingo.symbio.store.mongodb.journal.reader.MongoDBStreamReaderActor;
import io.vlingo.symbio.store.mongodb.journal.reader.SequenceOffset;
import org.bson.Document;

import java.time.Clock;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.stream.Collectors.toList;

public class MongoDBJournalActor extends Actor implements Journal<Document> {

    public static final String JOURNAL_COLLECTION_NAME = "symbio_journal";

    private final Clock clock = Clock.systemUTC();

    private SequenceOffset sequencePointer;

    private final Configuration configuration;
    private final MongoCollection<Document> journal;

    private final Map<String, JournalReader<DocumentEntry>> journalReaders;
    private final Map<String, StreamReader<Document>> streamReaders;

    private final JournalListener<Document> journalListener;

    public MongoDBJournalActor(JournalListener<Document> journalListener, Configuration configuration) {
        this.configuration = configuration;

        final MongoDatabase database = configuration.client().getDatabase(configuration.databaseName());
        journal = database.getCollection(JOURNAL_COLLECTION_NAME).withWriteConcern(configuration.writeConcern());
        journal.createIndex(new Document("sequence.id", 1).append("sequence.offset", 1), new IndexOptions().unique(true).name("_journal_unique_sequence_idx"));
        journal.createIndex(new Document("streamName", 1).append("hasSnapshot", 1).append("streamVersionEnd", -1), new IndexOptions().name("_stream_reader_index"));
        journal.createIndex(new Document("sequence.timestamp", 1).append("sequence.id", 1).append("sequence.offset", 1), new IndexOptions().name("_journal_reader_index"));

        this.journalListener = journalListener;

        this.journalReaders = new HashMap<>();
        this.streamReaders = new HashMap<>();

        initSequenceNumber(configuration.getJournalId());
    }

    private void initSequenceNumber(String journalId) {
        final Document result = journal.find(new Document("sequence.id", journalId)).sort(new Document("sequence.offset", -1)).first();
        if (result == null) {
            this.sequencePointer = new SequenceOffset(journalId);
        } else {
            long latestSequenceNumber = new JournalDocumentAdapter(result).getSequence().getOffset();
            this.sequencePointer = new SequenceOffset(journalId, latestSequenceNumber).next();
        }
    }

    public <S, ST> void append(String streamName, int fromStreamVersion, Source<S> source, AppendResultInterest interest, Object o) {
        appendAllWithInternal(streamName, fromStreamVersion, Collections.singletonList(source), null)
                .resolve(
                        error -> returnVoid(() ->
                                interest.appendResultedIn(Failure.of(error), streamName, fromStreamVersion, source, Optional.empty(), o)
                        ),
                        success -> returnVoid(() -> {
                            interest.appendResultedIn(Success.of(Result.Success), streamName, fromStreamVersion, source, Optional.empty(), o);
                            journalListener.appended(success._1.get(0));
                        })
                );
    }

    public <S, ST> void appendWith(String streamName, int fromStreamVersion, Source<S> source, ST snapshot, AppendResultInterest interest, Object o) {
        appendAllWithInternal(streamName, fromStreamVersion, Collections.singletonList(source), snapshot)
                .resolve(
                        error -> returnVoid(() ->
                                interest.appendResultedIn(Failure.of(error), streamName, fromStreamVersion, source, Optional.ofNullable(snapshot), o)
                        ),
                        success -> returnVoid(() -> {
                            interest.appendResultedIn(Success.of(Result.Success), streamName, fromStreamVersion, source, Optional.ofNullable(snapshot), o);
                            journalListener.appendedWith(success._1.get(0), success._2.orElse(null));
                        })
                );
    }

    public <S, ST> void appendAll(String streamName, int fromStreamVersion, List<Source<S>> sources, AppendResultInterest interest, Object o) {
        appendAllWithInternal(streamName, fromStreamVersion, sources, null)
                .resolve(
                        error -> returnVoid(() ->
                                interest.appendAllResultedIn(Failure.of(error), streamName, fromStreamVersion, sources, Optional.empty(), o)
                        ),
                        success -> returnVoid(() -> {
                            interest.appendAllResultedIn(Success.of(Result.Success), streamName, fromStreamVersion, sources, Optional.empty(), o);
                            journalListener.appendedAll(success._1);
                        })
                );
    }

    public <S, ST> void appendAllWith(String streamName, int fromStreamVersion, List<Source<S>> sources, ST snapshot, AppendResultInterest interest, Object o) {
        appendAllWithInternal(streamName, fromStreamVersion, sources, snapshot).
                resolve(
                        error -> returnVoid(() ->
                                interest.appendAllResultedIn(Failure.of(error), streamName, fromStreamVersion, sources, Optional.ofNullable(snapshot), o)
                        ),
                        success -> returnVoid(() -> {
                                    interest.appendAllResultedIn(Success.of(Result.Success), streamName, fromStreamVersion, sources, Optional.ofNullable(snapshot), o);
                                    journalListener.appendedAllWith(success._1, success._2.orElse(null));
                                }
                        )
                );
    }

    private <S, ST> Outcome<StorageException, Tuple2<List<Entry<Document>>, Optional<State<Document>>>> appendAllWithInternal(String streamName, int fromStreamVersion, List<Source<S>> sources, ST snapshot) {
        try {

            final int maxVersion = fromStreamVersion + sources.size() - 1;
            final AtomicInteger version = new AtomicInteger(fromStreamVersion);

            final List<Tuple2<Integer, Entry<Document>>> entries = sources.stream()
                    .map(s -> {
                        final int streamVersion = version.getAndIncrement();
                        return Tuple2.from(streamVersion, this.asEntry(s, streamVersion));
                    })
                    .collect(toList());

            final State<Document> state = snapshot == null ? null : asState(fromStreamVersion, snapshot);

            final JournalDocumentAdapter dbDocument = new JournalDocumentAdapter(
                    streamName,
                    JournalDocumentSequence.fromSequenceOffset(clock.millis(), sequencePointer),
                    fromStreamVersion,
                    maxVersion,
                    entries.stream()
                            .map(tuple -> JournalDocumentEntry.fromEntry(tuple._2, tuple._1))
                            .collect(toList()),
                    state == null ? null : JournalDocumentState.fromState(state)
            );

            final Document filter = new Document("streamName", streamName)
                    .append("streamVersionEnd", new Document("$gte", fromStreamVersion));
            final Document update = new Document("$setOnInsert", dbDocument.getDocument());

            final UpdateResult updateResult = journal.updateOne(filter, update, new UpdateOptions().upsert(true));

            if (updateResult.getModifiedCount() > 0) {
                // UH OH!
                throw new StorageException(Result.Failure, "EventStream has been mutated! This should not happen. Please report issue!");
            }

            boolean success = updateResult.getUpsertedId() != null;


            if (success) {
                this.sequencePointer = sequencePointer.next();
            } else {
                throw new StorageException(Result.ConcurrentyViolation, String.format("StreamVersion %s already exits in Stream %s!", fromStreamVersion, streamName));
            }

            return Success.of(Tuple2.from(entries.stream().map(t -> t._2).collect(toList()), Optional.ofNullable(state)));
        } catch (StorageException e) {
            return Failure.of(e);
        } catch (Exception e) {
            return Failure.of(new StorageException(Result.Error, "Unexpected error occurred.", e));
        }
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

    private Entry<Document> asEntry(Source<?> source, int streamVersion) {
//        final SourcedTypeRegistry sourcedTypeRegistry = getSourcedTypeRegistry();
//        final Entry<?> entry = sourcedTypeRegistry.info(SOURCED_CLASS_MISSING).entryAdapterProvider.asEntry(source);
//        if (!entryAdapters.containsKey(source.getClass())) {
//            throw new IllegalStateException(String.format("No EntryAdapter for source '%s' found", source.getClass()));
//        }
        final String id = String.format("%s-%s-%s", sequencePointer.getId(), sequencePointer.getOffset(), streamVersion);
        final Entry<?> entry = new DefaultDocumentEntryAdapter<>().toEntry(source, id);//)entryAdapters.get(source.getClass()).toEntry(source);

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

    private static Void returnVoid(Runnable runnable) {
        runnable.run();
        return null;
    }

}
