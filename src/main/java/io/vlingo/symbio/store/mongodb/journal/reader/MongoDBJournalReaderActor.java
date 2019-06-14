package io.vlingo.symbio.store.mongodb.journal.reader;

import com.google.gson.Gson;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import io.vlingo.common.Completes;
import io.vlingo.common.Tuple2;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.store.mongodb.Configuration;
import io.vlingo.symbio.store.mongodb.journal.JournalDocumentAdapter;
import io.vlingo.symbio.store.mongodb.journal.JournalDocumentAdapter.JournalDocumentEntry;
import io.vlingo.symbio.store.mongodb.journal.JournalDocumentAdapter.JournalDocumentSequence;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static io.vlingo.symbio.store.mongodb.journal.MongoDBJournalActor.JOURNAL_COLLECTION_NAME;
import static java.util.stream.Collectors.toList;

public class MongoDBJournalReaderActor extends AbstractMongoJournalReadingActor implements MultiSequenceJournalReader<Document> {

    private static final Gson CURSOR_SERIALIZER = new Gson();
    static final String JOURNAL_OFFSETS_COLLECTION_NAME = "symbio_journal_offsets";

    private final String name;

    private final MongoCollection<Document> journalOffsets;
    private final MongoCollection<Document> journal;

    private CursorToken currentCursorToken;

    private final GapResolver gapResolver;

    public MongoDBJournalReaderActor(String name, Configuration configuration) {
        this.name = name;

        final MongoDatabase database = configuration.client().getDatabase(configuration.databaseName());
        this.journal = database.getCollection(JOURNAL_COLLECTION_NAME);
        this.journalOffsets = database.getCollection(JOURNAL_OFFSETS_COLLECTION_NAME).withWriteConcern(configuration.writeConcern());

        initCurrentCursorToken();

        gapResolver = new TimeoutGapResolver(journal, 5, TimeUnit.SECONDS);
    }

    private void initCurrentCursorToken() {
        final Document result = journalOffsets.find(new Document("_id", this.name)).first();

        if (result == null) {
            this.currentCursorToken = CursorToken.beginning();
        } else {
            this.currentCursorToken = CURSOR_SERIALIZER.fromJson(result.getString("data"), CursorToken.class);
        }
    }

    public Completes<String> name() {
        return completes().with(name);
    }

    public Completes<Entry<Document>> readNext() {
        final List<Entry<Document>> result = readNextInternal(1, entries -> {
            if (entries.size() > 1) {
                throw new IllegalStateException();
            }
        });
        if (result.isEmpty()) {
            return completes().with(null);
        } else {
            return completes().with(result.get(0));
        }
    }

    public Completes<List<Entry<Document>>> readNext(int maxCount) {
        return completes().with(readNextInternal(maxCount, entries -> {
        }));
    }

    private List<Entry<Document>> readNextInternal(int maxCount, Consumer<List<Entry<Document>>> verifier) {
        final List<ResolvedGap> resolvedGaps = gapResolver.resolveGaps(currentCursorToken, maxCount);
        final List<Tuple2<EntrySequenceOffset, Entry<Document>>> gapEntries = handleResolved(resolvedGaps);

        int remainingEntries = maxCount;

        final List<Entry<Document>> result = new ArrayList<>(maxCount);

        CursorToken batchToken = currentCursorToken;

        for (Tuple2<EntrySequenceOffset, Entry<Document>> gapEntry : gapEntries) {
            if(remainingEntries == 0) break;
            if (!batchToken.isMissing(gapEntry._1)) continue;

            result.add(gapEntry._2);
            batchToken = batchToken.resolved(gapEntry._1);
            remainingEntries--;
        }

        if (remainingEntries > 0) {
            boolean queryCurrentTokenDocument = batchToken.hasCurrentDocumentMoreEntries();

            MongoCursor<Document> iterator = getDocumentIterator(remainingEntries, batchToken, queryCurrentTokenDocument);

            while (iterator.hasNext() && remainingEntries > 0) {
                final JournalDocumentAdapter nextDocument = new JournalDocumentAdapter(iterator.next());
                batchToken = batchToken.hasCurrentDocumentMoreEntries() ? batchToken : nextBatchToken(batchToken, nextDocument);
                final List<JournalDocumentEntry> entries = nextDocument.getEntries();
                while (batchToken.hasCurrentDocumentMoreEntries() && remainingEntries > 0) {
                    result.add(asEntry(entries.get(batchToken.getCurrentDocumentOffset().getEntryIndex())));
                    batchToken = batchToken.nextIndex();
                    remainingEntries--;
                }
                if (queryCurrentTokenDocument) {
                    iterator = getDocumentIterator(remainingEntries, batchToken, false);
                    queryCurrentTokenDocument = false;
                }
            }
        }
        verifier.accept(result);
        updateCurrentCursorToken(batchToken);
        return result;
    }

    private List<Tuple2<EntrySequenceOffset, Entry<Document>>> handleResolved(List<ResolvedGap> gaps) {
        return gaps.stream()
                .map(ResolvedGap::getData)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .flatMap(doc -> {
                    final List<Tuple2<EntrySequenceOffset, Entry<Document>>> entries = new ArrayList<>(doc.getEntries().size());
                    for (int i = 0; i < doc.getEntries().size(); i++) {
                        final JournalDocumentEntry entry = doc.getEntries().get(i);
                        final EntrySequenceOffset entrySequenceOffset = new EntrySequenceOffset(doc.getSequence().toSequenceOffset(), i, doc.getEntries().size());
                        entries.add(Tuple2.from(entrySequenceOffset, asEntry(entry)));
                    }
                    return entries.stream();
                })
                .collect(toList());
    }

    private MongoCursor<Document> getDocumentIterator(int maxEntries, CursorToken batchToken, boolean queryCurrentTokenDocument) {
        final List<Document> subQueries = new ArrayList<>();
        final List<String> knownSequenceIds = new ArrayList<>();

        if (queryCurrentTokenDocument) {
            subQueries.add(JournalDocumentAdapter.queryFor(batchToken.getCurrentDocumentOffset()));
        } else {
            currentCursorToken.forEachSequenceHead((pointer, isCurrent) -> {
                knownSequenceIds.add(pointer.getId());
                subQueries.add(
                        new Document("sequence.id", pointer.getId())
                                .append("sequence.offset", new Document("$gt", pointer.getOffset()))
                );
            });
            subQueries.add(new Document("sequence.id", new Document("$nin", knownSequenceIds)));
        }

        final Document query = new Document("$or", subQueries);

        return journal.find(query).sort(new Document("sequence", 1)).limit(maxEntries).iterator();
    }

    private static CursorToken nextBatchToken(CursorToken batchToken, JournalDocumentAdapter nextDocument) {
        final SequenceOffset nextSequencePointer = extractSequenceOffset(nextDocument);
        final List<JournalDocumentEntry> entries = nextDocument.getEntries();

        final Set<SequenceOffset> gaps = batchToken.findGaps(nextSequencePointer);

        return batchToken
                .withCurrentDocumentOffset(new EntrySequenceOffset(nextSequencePointer, 0, entries.size()))
                .detected(gaps);
    }

    public void rewind() {
        journalOffsets.findOneAndDelete(new Document("name", this.name));
        this.currentCursorToken = CursorToken.beginning();
    }

    public Completes<String> seekTo(String id) {
        switch (id) {
            case Beginning:
                return seekToBeginning();
            case Query:
                if (currentCursorToken.isBeginning()) {
                    return completes().with(null);
                } else {
                    return completes().with(asEntryIdString(currentCursorToken.getCurrentDocumentOffset()));
                }
            case End:
                return seekToEnd();
            default:
                return parseSequenceId(id)
                        .map(validPointer -> {
                            final SequenceOffset offset = validPointer._1;
                            final Document documentForOffset = journal.find(JournalDocumentAdapter.queryFor(offset)).first();
                            if (documentForOffset == null) {
                                return seekToEnd();
                            }

                            final Document offsetFilter = new Document("sequence.timestamp", new Document("$gte", documentForOffset.get("sequence.timestamp")).append("$not", new Document("sequence.id", offset.getId()).append("sequence.offset", offset.getOffset())));
                            final List<SequenceOffset> maxOffsets = getMaxOffsets(offsetFilter).collect(toList());
                            final JournalDocumentAdapter documentAdapter = new JournalDocumentAdapter(documentForOffset);

                            CursorToken newToken = CursorToken.beginning();
                            for (SequenceOffset o : maxOffsets) {
                                newToken = newToken.withSequenceHead(new EntrySequenceOffset(o, 0, 0));
                            }
                            newToken.withCurrentDocumentOffset(new EntrySequenceOffset(offset, validPointer._2, documentAdapter.getEntries().size()));

                            updateCurrentCursorToken(newToken);

                            return completes().with(id);
                        })
                        .orElseGet(this::seekToEnd);
        }
    }

    private static EntrySequenceOffset extractSequenceOffset(JournalDocumentAdapter entry) {
        final JournalDocumentSequence sequence = entry.getSequence();
        final String sequenceId = sequence.getId();
        final Long sequenceOffset = sequence.getOffset();
        return new EntrySequenceOffset(sequenceId, sequenceOffset, 0, entry.getEntries().size());
    }

    private String asEntryIdString(EntrySequenceOffset offset) {
        final String sequenceId = offset.getId();
        final Long sequenceOffset = offset.getOffset();

        return String.format("%s-%s-%s", sequenceId, sequenceOffset, offset.getEntryIndex());
    }

    private Optional<Tuple2<SequenceOffset, Integer>> parseSequenceId(String id) {
        if (id == null) {
            return Optional.empty();
        }

        final String[] splited = id.split("-");
        if (splited.length < 2 || splited.length > 3) {
            return Optional.empty();
        }
        try {
            final String sequenceId = splited[0];
            final long offset = Long.parseLong(splited[1]);
            final int idx = splited.length == 3 ? Integer.parseInt(splited[2]) : 0;
            return Optional.of(Tuple2.from(new SequenceOffset(sequenceId, offset), idx));
        } catch (NumberFormatException e) {
            logger().log("Unable to parse sequence id: " + id, e);
            return Optional.empty();
        }
    }

    private Completes<String> seekToEnd() {
        final CursorToken token = CursorToken.withSequenceOffsets(getMaxOffsets(null).toArray(SequenceOffset[]::new));
        updateCurrentCursorToken(token);
        return completes().with(asEntryIdString(token.getCurrentDocumentOffset()));
    }

    private Completes<String> seekToBeginning() {
        final Document first = journal.find().sort(new Document("sequence.timestamp", 1)).first();
        if (first == null) return completes().with(null);
        rewind();
        final EntrySequenceOffset sequenceOffset = extractSequenceOffset(new JournalDocumentAdapter(first));
        return completes().with(asEntryIdString(sequenceOffset));
    }

    @Override
    public Completes<Set<SequenceOffset>> seekTo(SequenceOffset... offsets) {

        final CursorToken.CursorTokenBuilder builder = CursorToken.builder();
        Arrays.stream(offsets)
                .map(SequenceOffset::previous)
                .forEach(offset -> builder.sequenceHead(offset.getId(), offset));
        final CursorToken token = builder.build();
        updateCurrentCursorToken(token);
        return completes().with(new HashSet<>(token.getSequenceHeads().values()));
    }

    private void updateCurrentCursorToken(CursorToken newCurrent) {
        this.currentCursorToken = newCurrent;

        writeCurrentToken(newCurrent);
    }

    private void writeCurrentToken(CursorToken newCurrent) {
        journalOffsets.updateMany(new Document("_id", this.name), new Document("$set", new Document("data", CURSOR_SERIALIZER.toJson(newCurrent))), new UpdateOptions().upsert(true));
    }

    private Stream<SequenceOffset> getMaxOffsets(Document filter) {
        final List<Document> aggregations = new ArrayList<>();

        if (filter != null) {
            aggregations.add(new Document("$match", filter));
        }
        aggregations.add(new Document("$group", new Document("_id", "$sequence.id").append("latest", new Document("$max", "$sequence.offset"))));

        final AggregateIterable<Document> maxOffsets = journal.aggregate(aggregations);

        return StreamSupport.stream(maxOffsets.spliterator(), false)
                .map(offset -> new SequenceOffset(offset.getString("_id"), offset.getLong("latest")));
    }

}
