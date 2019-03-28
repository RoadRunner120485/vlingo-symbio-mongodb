package io.vlingo.symbio.store.mongodb.journal;

import com.google.gson.Gson;
import io.vlingo.symbio.Metadata;
import io.vlingo.symbio.State;
import io.vlingo.symbio.StateAdapter;
import org.bson.Document;

public class DefaultDocumentStateAdapter<T> implements StateAdapter<T, State<Document>> {

    private final Gson gson;
    private final int typeVersion;

    public DefaultDocumentStateAdapter() {
        this(0);
    }

    public DefaultDocumentStateAdapter(int typeVersion) {
        this(new Gson(), typeVersion);
    }

    public DefaultDocumentStateAdapter(Gson gson, int typeVersion) {
        this.gson = gson;
        this.typeVersion = typeVersion;
    }

    private Class<?> sourceType(State<Document> entry) {
        try {
            return Class.forName(entry.type);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(String.format("Could not load Class '%s' from classpath", entry.type), e);
        } catch (ClassCastException e) {
            throw new IllegalStateException(String.format("Class '%s' is not a subclass of Source<?>", entry.type), e);
        }
    }

    @Override
    public int typeVersion() {
        return typeVersion;
    }

    @Override
    public T fromRawState(State<Document> state) {
        return (T) gson.fromJson(state.data.getString("json"), sourceType(state));
    }

    @Override
    public State<Document> toRawState(Object state, int stateVersion, Metadata metadata) {
        return new DocumentState(state.getClass(), typeVersion, new Document("json", gson.toJson(state)), stateVersion, metadata);
    }

    @Override
    public State<Document> toRawState(Object state, int stateVersion) {
        return toRawState(state, stateVersion, Metadata.nullMetadata());
    }
}
