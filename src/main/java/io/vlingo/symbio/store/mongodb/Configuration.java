package io.vlingo.symbio.store.mongodb;

import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;

public class Configuration {

    private final String journalId;
    private final MongoClient client;
    private final String databaseName;
    private final WriteConcern writeConcern;

    public Configuration(String journalId, MongoClient client, String databaseName) {
        this(journalId, client, databaseName, WriteConcern.W2.withJournal(true));
    }

    public Configuration(String journalId, MongoClient client, String databaseName, WriteConcern writeConcern) {
        this.journalId = journalId;
        this.client = client;
        this.databaseName = databaseName;
        this.writeConcern = writeConcern;
    }

    public String getJournalId() {
        return journalId;
    }

    public MongoClient client() {
        return client;
    }

    public String datebaseName() {
        return databaseName;
    }

    public WriteConcern writeConcern() {
        return writeConcern;
    }
}
