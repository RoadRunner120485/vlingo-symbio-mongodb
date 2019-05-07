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
import org.bson.Document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static io.vlingo.symbio.store.mongodb.journal.MongoDBJournalActor.JOURNAL_COLLECTION_NAME;
import static java.util.stream.Collectors.toList;

public class MongoDBJournalReaderActor extends AbstractMongoJournalReadingActor implements MultiSequenceJournalReader<Document> {

    private static final Gson CURSOR_SERIALIZER = new Gson();
    static final String JOURNAL_OFFSETS_COLLECTION_NAME = "symbio_journal_offsets";

    private final String name;
    private final GapResolver gapResolver;
    private CursorToken currentCursorToken;

    private final MongoCollection<Document> journalOffsets;
    private final MongoCollection<Document> journal;

    public MongoDBJournalReaderActor(String name, Configuration configuration) {
        this.name = name;

        final MongoDatabase database = configuration.client().getDatabase(configuration.datebaseName());
        this.journal = database.getCollection(JOURNAL_COLLECTION_NAME);
        this.journalOffsets = database.getCollection(JOURNAL_OFFSETS_COLLECTION_NAME).withWriteConcern(configuration.writeConcern());

        initCurrentCursorToken();

//        gapResolver = stage().actorFor(GapResolver.class, Definition.has(GapResolverActor.class, Definition.parameters(this.name)));
        gapResolver = new TimeoutGapResolver(journal, 5, TimeUnit.SECONDS);
    }

    private void initCurrentCursorToken() {
        final Document result = journalOffsets.find(new Document("_id", this.name)).first();

        if (result == null) {
            this.currentCursorToken = CursorToken.beginning();
        } else {
            this.currentCursorToken = CURSOR_SERIALIZER.fromJson(result.getString("data"), CursorToken.class); // Tuple3.from(result.getObjectId("offsetId"), result.getInteger("offsetIndex"), result.getBoolean("hasMoreEntries"));
        }
    }

    public Completes<String> name() {
        return completes().with(name);
    }

    public Completes<Entry<Document>> readNext() {
        return readNext(1)
                .andThen(list -> list.size() == 1 ? list.get(0) : null);
    }

    public Completes<List<Entry<Document>>> readNext(int maxCount) {
        final List<Entry<Document>> result = new ArrayList<>(maxCount);
        final List<Tuple2<SequenceOffset, Optional<Document>>> resolvedGaps = gapResolver.resolveGaps(currentCursorToken, maxCount);

        final Tuple2<CursorToken, List<Entry<Document>>> gapEntriesTuple = handleResolvedGaps(resolvedGaps);
        result.addAll(gapEntriesTuple._2);

        int maxEntries = maxCount - resolvedGaps.size();

        CursorToken batchToken = gapEntriesTuple._1;
        boolean queryCurrentTokenDocument = batchToken.hasCurrentDocumentMoreEntries();

        MongoCursor<Document> iterator = getDocumentIterator(maxEntries, batchToken, queryCurrentTokenDocument);
        while (iterator.hasNext() && result.size() < maxEntries) {
            final Document nextDocument = iterator.next();
            batchToken = batchToken.hasCurrentDocumentMoreEntries() ? batchToken : nextBatchToken(batchToken, nextDocument);
            final List<Document> entries = nextDocument.getList("entries", Document.class);
            while (batchToken.hasCurrentDocumentMoreEntries() && result.size() < maxEntries) {
                final Integer idx = batchToken.getCurrentDocumentIndex();
                final String id = asEntryIdString(batchToken.getCurrentDocumentOffset(), idx);

                result.add(asEntry(id, entries.get(batchToken.getCurrentDocumentIndex())));
                batchToken = batchToken.nextIndex();
            }
            if (queryCurrentTokenDocument) {
                iterator = getDocumentIterator(maxEntries - result.size(), batchToken, false);
                queryCurrentTokenDocument = false;
            }
        }
        updateCurrentCursorToken(batchToken);
        return completes().with(result);
    }

    private Tuple2<CursorToken, List<Entry<Document>>> handleResolvedGaps(List<Tuple2<SequenceOffset, Optional<Document>>> resolvedGaps) {
        final Function<Document, Stream<Entry<Document>>> documentMapper = document -> {
            final String sequence = document.getString("sequence.id");
            final Long offset = document.getLong("sequence.offset");
            final List<Document> entries = document.getList("entries", Document.class);
            return IntStream.range(0, entries.size())
                    .boxed()
                    .map(i -> asEntry(asEntryIdString(new SequenceOffset(sequence, offset), i), entries.get(i)));
        };


        final Set<SequenceOffset> gapOffsets = new HashSet<>();
        final List<Entry<Document>> result = resolvedGaps.stream()
                .peek(tuple -> gapOffsets.add(tuple._1))
                .map(tuple -> tuple._2)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .flatMap(documentMapper)
                .collect(toList());

        return Tuple2.from(currentCursorToken.removeGaps(gapOffsets), result);
    }

    private MongoCursor<Document> getDocumentIterator(int maxEntries, CursorToken batchToken, boolean queryCurrentTokenDocument) {
        final List<Document> subQueries = new ArrayList<>();
        final List<String> knownSequenceIds = new ArrayList<>();

        if (queryCurrentTokenDocument) {
            subQueries.add(new Document("sequence.id", batchToken.getCurrentDocumentOffset().getId()).append("sequence.offset", batchToken.getCurrentDocumentOffset().getOffset()));
        } else {
            currentCursorToken.forEachSequence((pointer, isCurrent) -> {
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

    private static CursorToken nextBatchToken(CursorToken batchToken, Document nextDocument) {
        final SequenceOffset nextSequencePointer = extractSequenceOffset(nextDocument);
        final List<Document> entries = nextDocument.getList("entries", Document.class);

        final Set<SequenceOffset> gaps = batchToken.findGaps(nextSequencePointer);

        return batchToken
                .withCurrentDocumentOffset(nextSequencePointer, 0, entries.size())
                .addGaps(gaps);
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
                    return completes().with(asEntryIdString(currentCursorToken.getCurrentDocumentOffset(), currentCursorToken.getCurrentDocumentIndex()));
                }
            case End:
                return seekToEnd();
            default:
                return parseSequenceId(id)
                        .map(validPointer -> {
                            final SequenceOffset offset = validPointer._1;
                            final Document documentForOffset = journal.find(new Document("sequence.id", offset.getId()).append("sequence.offset", offset.getOffset())).first();
                            if (documentForOffset == null) {
                                return seekToEnd();
                            }
                            final Document offsetFilter = new Document("sequence.timestamp", new Document("$gte", documentForOffset.get("sequence.timestamp")).append("$not", new Document("sequence.id", offset.getId()).append("sequence.offset", offset.getOffset())));
                            final List<SequenceOffset> maxOffsets = getMaxOffsets(offsetFilter).collect(toList());

                            CursorToken newToken = CursorToken.beginning();
                            for (SequenceOffset o : maxOffsets) {
                                newToken = newToken.withCurrentDocumentOffset(o, 0, 0);
                            }
                            newToken.withCurrentDocumentOffset(offset, validPointer._2, documentForOffset.getList("entries", Document.class).size());

                            updateCurrentCursorToken(newToken);

                            return completes().with(id);
                        })
                        .orElseGet(this::seekToEnd);
        }
    }

    private static SequenceOffset extractSequenceOffset(Document entry) {
        final Document sequence = entry.get("sequence", Document.class);
        final String sequenceId = sequence.getString("id");
        final Long sequenceOffset = sequence.getLong("offset");
        return new SequenceOffset(sequenceId, sequenceOffset);
    }

    private String asEntryIdString(SequenceOffset offset, Integer idx) {
        final String sequenceId = offset.getId();
        final Long sequenceOffset = offset.getOffset();

        return String.format("%s-%s-%s", sequenceId, sequenceOffset, idx);
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
        return completes().with(asEntryIdString(token.getCurrentDocumentOffset(), token.getCurrentDocumentIndex()));
    }

    private Completes<String> seekToBeginning() {
        final Document first = journal.find().sort(new Document("sequence.timestamp", 1)).first();
        if (first == null) return completes().with(null);
        rewind();
        final SequenceOffset sequenceOffset = extractSequenceOffset(first);
        return completes().with(asEntryIdString(sequenceOffset, 0));
    }

    @Override
    public Completes<Set<SequenceOffset>> seekTo(SequenceOffset... offsets) {

        final CursorToken.CursorTokenBuilder builder = CursorToken.builder();
        Arrays.stream(offsets)
                .map(SequenceOffset::previous)
                .forEach(offset -> builder.topOfSequencePointer(offset.getId(), offset));
        final CursorToken token = builder.build();
        updateCurrentCursorToken(token);
        return completes().with(new HashSet<>(token.getTopOfSequencePointers().values()));
    }

    private void updateCurrentCursorToken(CursorToken newCurrent) {
        this.currentCursorToken = newCurrent;

        writeCurrentToken(newCurrent);
    }

    private void writeCurrentToken(CursorToken newCurrent) {
        journalOffsets.updateMany(new Document("_id", this.name), new Document("$set", new Document("data", CURSOR_SERIALIZER.toJson(newCurrent))), new UpdateOptions().upsert(true));
    }

    private Stream<SequenceOffset> getMaxOffsets() {
        return getMaxOffsets(null);
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
