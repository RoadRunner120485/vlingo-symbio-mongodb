package io.vlingo.symbio.store.mongodb.journal;

import com.google.gson.Gson;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.EntryAdapter;
import io.vlingo.symbio.Metadata;
import io.vlingo.symbio.Source;
import org.bson.Document;

public class DefaultDocumentEntryAdapter<T extends Source<?>> implements EntryAdapter<T, Entry<Document>> {

    private final Gson gson;

    public DefaultDocumentEntryAdapter() {
        this(new Gson());
    }

    public DefaultDocumentEntryAdapter(Gson gson) {
        this.gson = gson;
    }

    @Override
    public T fromEntry(Entry<Document> entry) {
        return (T) gson.fromJson(entry.entryData.getString("json"), sourceType(entry));
    }

    @SuppressWarnings("unchecked")
    private Class<Source<?>> sourceType(Entry<Document> entry) {
        Class<Source<?>> entryType;
        try {
            entryType = (Class<Source<?>>) Class.forName(entry.type);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(String.format("Could not load Class '%s' from classpath", entry.type), e);
        } catch (ClassCastException e) {
            throw new IllegalStateException(String.format("Class '%s' is not a subclass of Source<?>", entry.type), e);
        }
        return entryType;
    }

    @Override
    public Entry<Document> toEntry(T source) {
        return new DocumentEntry(source.getClass(), 1, new Document("json", gson.toJson(source)), Metadata.nullMetadata());
    }

    @Override
    public Entry<Document> toEntry(T source, String id) {
        return new DocumentEntry(id, source.getClass(), 1, new Document("json", gson.toJson(source)), Metadata.nullMetadata());
    }
}
