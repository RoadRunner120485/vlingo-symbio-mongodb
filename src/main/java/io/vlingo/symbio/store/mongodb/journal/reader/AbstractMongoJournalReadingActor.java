package io.vlingo.symbio.store.mongodb.journal.reader;

import com.google.gson.Gson;
import io.vlingo.actors.Actor;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.Metadata;
import io.vlingo.symbio.State;
import io.vlingo.symbio.store.mongodb.journal.DocumentEntry;
import io.vlingo.symbio.store.mongodb.journal.DocumentState;
import org.bson.Document;

abstract class AbstractMongoJournalReadingActor extends Actor {

    private final Gson gson = new Gson();

    protected Entry<Document> asEntry(String id, Document event) {
        try {
            String metadata = event.getString("metadata");
            Document entryData = event.get("document", Document.class);
            String type = event.getString("type");
            int typeVersion = event.getInteger("typeVersion");

            Class<?> clazz = Class.forName(type);

            return new DocumentEntry(id, clazz, typeVersion, entryData, gson.fromJson(metadata, Metadata.class));
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    protected State<Document> asState(String streamName, Document stateDocument) {
        try {
            Document metadata = stateDocument.get("metadata", Document.class);
            Document data = stateDocument.get("document", Document.class);
            String type = stateDocument.getString("type");
            int typeVersion = stateDocument.getInteger("typeVersion");
            int dataVersion = stateDocument.getInteger("dataVersion");

            Class<?> clazz = Class.forName(type);

            return new DocumentState(streamName, clazz, typeVersion, data, dataVersion, gson.fromJson(metadata.toJson(), Metadata.class));
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

}
