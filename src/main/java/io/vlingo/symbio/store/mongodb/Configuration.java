package io.vlingo.symbio.store.mongodb;

import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;

public class Configuration {

    private final MongoClient client;
    private final String databaseName;
    private final WriteConcern writeConcern;

    public Configuration(MongoClient client, String databaseName, WriteConcern writeConcern) {
        this.client = client;
        this.databaseName = databaseName;
        this.writeConcern = writeConcern;
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
