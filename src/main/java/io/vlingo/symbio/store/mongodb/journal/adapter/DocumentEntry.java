package io.vlingo.symbio.store.mongodb.journal.adapter;

import io.vlingo.symbio.BaseEntry;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.Metadata;
import org.bson.Document;

public class DocumentEntry extends BaseEntry<Document> {

    public DocumentEntry(String id, Class<?> type, int typeVersion, Document entryData, Metadata metadata) {
        super(id, type, typeVersion, entryData, metadata);
    }

    @Override
    public Entry<Document> withId(String id) {
        throw new UnsupportedOperationException();
    }
}
