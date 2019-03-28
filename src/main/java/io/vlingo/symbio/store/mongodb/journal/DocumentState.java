package io.vlingo.symbio.store.mongodb.journal;

import io.vlingo.symbio.Metadata;
import io.vlingo.symbio.State;
import org.bson.Document;

public class DocumentState extends State<Document> {

    public static final DocumentState Null = new DocumentState();

//    private static final Document EmptyStateData = new Document();

    private final Class<?> typeClass;

    public DocumentState(String id, Class<?> type, int typeVersion, Document data, int dataVersion, Metadata metadata) {
        super(id, type, typeVersion, data, dataVersion, metadata);
        this.typeClass = type;
    }

    public DocumentState(Class<?> type, int typeVersion, Document data, int dataVersion, Metadata metadata) {
        this(NoOp, type, typeVersion, data, dataVersion, metadata);
    }

    public DocumentState(Class<?> type, int typeVersion, Document data, int dataVersion) {
        this(NoOp, type, typeVersion, data, dataVersion);
    }

    public DocumentState(String id, Class<?> type, int typeVersion, Document data, int dataVersion) {
        super(id, type, typeVersion, data, dataVersion);
        this.typeClass = type;
    }

    private DocumentState() {
        super(NoOp, Object.class, 1, new Document(), 1, Metadata.nullMetadata());
        this.typeClass = Object.class;
    }

    @Override
    public ObjectState<Document> asObjectState() {
        return new State.ObjectState<>(id, typeClass, typeVersion, data, dataVersion, metadata);
    }

    @Override
    public boolean isEmpty() {
        return data.isEmpty();
    }

    @Override
    public boolean isObject() {
        return true;
    }
}
