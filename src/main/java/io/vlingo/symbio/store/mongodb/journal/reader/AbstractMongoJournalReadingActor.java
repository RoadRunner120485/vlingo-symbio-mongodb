package io.vlingo.symbio.store.mongodb.journal.reader;

import io.vlingo.actors.Actor;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.State;
import io.vlingo.symbio.store.mongodb.journal.adapter.DocumentEntry;
import io.vlingo.symbio.store.mongodb.journal.adapter.DocumentState;
import io.vlingo.symbio.store.mongodb.journal.JournalDocumentAdapter;
import org.bson.Document;

abstract class AbstractMongoJournalReadingActor extends Actor {


    protected Entry<Document> asEntry(JournalDocumentAdapter.JournalDocumentEntry event) {
        try {
            Document entryData = event.getEntry();
            String type = event.getType();
            int typeVersion = event.getTypeVersion();

            Class<?> clazz = Class.forName(type);

            return new DocumentEntry(event.getId(), clazz, typeVersion, entryData, event.getMetadata());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    protected State<Document> asState(String streamName, JournalDocumentAdapter.JournalDocumentState stateDocument) {
        try {

            Class<?> clazz = Class.forName(stateDocument.getType());

            return new DocumentState(streamName, clazz, stateDocument.getTypeVersion(), stateDocument.getState(), stateDocument.getDataVersion(), stateDocument.getMetadata());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

}
