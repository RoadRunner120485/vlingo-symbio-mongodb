package io.vlingo.symbio.store.mongodb.journal;

import io.vlingo.symbio.BaseEntry;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.Metadata;
import org.bson.Document;

public class DocumentEntry extends BaseEntry<Document> {

    public DocumentEntry(String id, Class<?> type, int typeVersion, Document entryData, Metadata metadata) {
        super(id, type, typeVersion, entryData, metadata);
    }

    public DocumentEntry(String id, Class<?> type, int typeVersion, Document entryData) {
        super(id, type, typeVersion, entryData);
    }

    public DocumentEntry(Class<?> type, int typeVersion, Document entryData, Metadata metadata) {
        super(type, typeVersion, entryData, metadata);
    }

    @Override
    public Entry<Document> withId(String id) {
        return new DocumentEntry(id, typed(), typeVersion(), entryData(), metadata());
    }
}
