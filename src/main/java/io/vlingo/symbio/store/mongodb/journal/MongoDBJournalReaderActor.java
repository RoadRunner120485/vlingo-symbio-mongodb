package io.vlingo.symbio.store.mongodb.journal;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import io.vlingo.common.Completes;
import io.vlingo.common.Tuple3;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.store.journal.JournalReader;
import io.vlingo.symbio.store.mongodb.Configuration;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.List;

public class MongoDBJournalReaderActor extends AbstractMongoJournalReadingActor implements JournalReader<Document> {

    static final String JOURNAL_OFFSETS_COLLECTION_NAME = "symbio_journal_offsets";

    private final String name;

    private final MongoCollection<Document> journalOffsets;
    private final MongoCollection<Document> journal;

    public MongoDBJournalReaderActor(String name, Configuration configuration) {
        this.name = name;

        final MongoDatabase database = configuration.client().getDatabase(configuration.datebaseName());
        this.journal = database.getCollection(MongoDBJournalActor.JOURNAL_COLLECTION_NAME);
        this.journalOffsets = database.getCollection(JOURNAL_OFFSETS_COLLECTION_NAME).withWriteConcern(configuration.writeConcern());
    }


    public Completes<String> name() {
        return completes().with(name);
    }

    public Completes<Entry<Document>> readNext() {
        return readNext(1)
                .andThen(list -> list.size() == 1 ? list.get(0) : null);
    }

    public Completes<List<Entry<Document>>> readNext(int maxEntries) {
        final Tuple3<ObjectId, Integer, Boolean> currentOffset = getCurrentOffset();
        final Boolean hasMoreEntries = currentOffset == null ? false : currentOffset._3;
        final int newEntryIndex = hasMoreEntries ? currentOffset._2 + 1 : 0;

        final Document query = currentOffset == null
                ? new Document()
                : hasMoreEntries
                ? new Document("_id", currentOffset._1)
                : new Document("_id", new Document("$gt", currentOffset._1));

        final MongoCursor<Document> iterator = journal.find(query).sort(new Document("_id", 1)).limit(maxEntries).iterator();

        final List<Entry<Document>> result = new ArrayList<>(maxEntries);

        Document currentDocument = null;
        int currentEntryIndex = newEntryIndex;

        while(iterator.hasNext() && result.size() < maxEntries) {
            currentDocument = iterator.next();
            final List<Document> entries = currentDocument.getList("entries", Document.class);
            for (int i = currentEntryIndex; i < entries.size(); i++) {
                result.add(asEntry(entries.get(i)));
                currentEntryIndex++;
                if (result.size() == maxEntries) break;

            }
            currentEntryIndex = 0;
        }

        if (currentDocument != null) {
            updateCurrentOffset(currentDocument.getObjectId("_id"), currentEntryIndex, currentDocument.getList("entries", Document.class).size() > currentEntryIndex + 1);
        }
        return completes().with(result);
    }

    public void rewind() {
        journalOffsets.findOneAndDelete(new Document("name", this.name));
    }

    public Completes<String> seekTo(String s) {
        switch (s) {
            case Beginning:
                final Document first = journal.find().sort(new Document("_id", 1)).first();
                if (first == null) return completes().with(null);
                rewind();
                return completes().with(first.getList("entries", Document.class).get(0).getObjectId("_id").toString());
            case Query:
                final Tuple3<ObjectId, Integer, Boolean> currentOffset = getCurrentOffset();
                if (currentOffset == null) return completes().with(null);
                final Document current = journal.find(new Document("_id", currentOffset._1)).first();
                return completes().with(current.getList("entries", Document.class).get(0).getObjectId("_id").toString());
            case End:
                final Document last = journal.find().sort(new Document("_id", -1)).first();
                if (last == null) return completes().with(null);
                final List<Document> lastEntries = last.getList("entries", Document.class);
                return completes().with(lastEntries.get(lastEntries.size() -1).getObjectId("_id").toString());
            default:
                final Document specific = journal.find(new Document("entries._id", new ObjectId(s))).first();
                final List<Document> specificEntries = specific.getList("entries", Document.class);

                boolean updated = false;

                for (int i = 0; i < specificEntries.size(); i++) {
                    final Document specificEntry = specificEntries.get(i);
                    if (specificEntry.getObjectId("_id").equals(new ObjectId(s))) {
                        updateCurrentOffset(specific.getObjectId("_id"), i, specificEntries.size() > i + 1);
                        updated = true;
                        break;
                    }
                }

                return updated ? completes().with(s) : Completes.withFailure();
        }
    }

    private Tuple3<ObjectId, Integer, Boolean> getCurrentOffset() {
        final Document result = journalOffsets.find(new Document("name", this.name)).first();

        if (result == null) {
            return null;
        } else {
            return Tuple3.from(result.getObjectId("offsetId"), result.getInteger("offsetIndex"), result.getBoolean("hasMoreEntries"));
        }
    }


    private void updateCurrentOffset(ObjectId currentDocumentId, Integer currentIndex, Boolean hasMoreEntries) {
        journalOffsets.insertOne(new Document("name", this.name).append("documentId", currentDocumentId).append("entryIndex", currentIndex).append("hasMoreEntries", hasMoreEntries));
    }
}
