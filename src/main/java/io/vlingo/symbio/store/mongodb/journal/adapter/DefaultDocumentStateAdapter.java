package io.vlingo.symbio.store.mongodb.journal.adapter;

import com.google.gson.Gson;
import io.vlingo.symbio.Metadata;
import io.vlingo.symbio.State;
import io.vlingo.symbio.StateAdapter;
import org.bson.Document;

public class DefaultDocumentStateAdapter<T> implements StateAdapter<T, State<Document>> {

    private final Gson gson;
    private final int typeVersion;

    public DefaultDocumentStateAdapter() {
        this(1);
    }

    public DefaultDocumentStateAdapter(int typeVersion) {
        this(new Gson(), typeVersion);
    }

    public DefaultDocumentStateAdapter(Gson gson, int typeVersion) {
        this.gson = gson;
        this.typeVersion = typeVersion;
    }

    private Class<T> sourceType(State<Document> entry) {
        try {
            return (Class<T>) Class.forName(entry.type);
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
        return fromRawState(state, sourceType(state));
    }

    @Override
    public <ST> ST fromRawState(State<Document> state, Class<ST> stateType) {
        return gson.fromJson(state.data.getString("json"), stateType);
    }

    @Override
    public State<Document> toRawState(String id, T state, int stateVersion, Metadata metadata) {
        return new DocumentState(id, state.getClass(), typeVersion, new Document("json", gson.toJson(state)), stateVersion, metadata);
    }
}
