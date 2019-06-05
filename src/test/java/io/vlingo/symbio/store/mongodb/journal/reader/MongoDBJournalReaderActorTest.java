package io.vlingo.symbio.store.mongodb.journal.reader;

import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.vlingo.actors.Definition;
import io.vlingo.actors.World;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.Metadata;
import io.vlingo.symbio.store.journal.JournalReader;
import io.vlingo.symbio.store.mongodb.Configuration;
import io.vlingo.symbio.store.mongodb.journal.EmbeddedMongoDbExtension;
import io.vlingo.symbio.store.mongodb.journal.JournalDocumentAdapter;
import io.vlingo.symbio.store.mongodb.journal.JournalDocumentAdapter.JournalDocumentEntry;
import io.vlingo.symbio.store.mongodb.journal.JournalDocumentAdapter.JournalDocumentSequence;
import io.vlingo.symbio.store.mongodb.journal.MongoDBJournalActor;
import io.vlingo.symbio.store.mongodb.journal.VlingoExtension;
import lombok.val;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(EmbeddedMongoDbExtension.class)
@ExtendWith(VlingoExtension.class)
@DisplayName("The MongoDBJournalReaderActor")
class MongoDBJournalReaderActorTest {

    private final JournalReader<Entry<Document>> underTest;
    private final MongoCollection<Document> journal;
    private final MongoCollection<Document> offsets;

    MongoDBJournalReaderActorTest(MongoClient mongo, World world) {
        val configuration = new Configuration(
                "counter_journal_1",
//        NoopConfigurationInterest, // You will need to create your own ConfigurationInterest
                mongo,
                "vlingo",
                WriteConcern.ACKNOWLEDGED
        );

        final Class<JournalReader<Entry<Document>>> type = (Class) JournalReader.class;
        this.underTest = world.actorFor(type, Definition.has(
                MongoDBJournalReaderActor.class,
                Definition.parameters("just_the_name", configuration)
        ));

        final MongoDatabase vlingoDb = mongo.getDatabase("vlingo");
        journal = vlingoDb.getCollection(MongoDBJournalActor.JOURNAL_COLLECTION_NAME);
        offsets = vlingoDb.getCollection(MongoDBJournalReaderActor.JOURNAL_OFFSETS_COLLECTION_NAME);
    }

    @AfterEach
    void cleanDatabase() {
        journal.deleteMany(new Document());
        offsets.deleteMany(new Document());
    }

    @Test
    void name() {
        assertThat(underTest.name().await()).isEqualTo("just_the_name");
    }

    @Nested
    @DisplayName("when invoking readNext should")
    class VerifyReadNext {

        @Test
        void readNext() {
            final JournalDocumentAdapter collectionEntry = JournalDocumentAdapter.builder()
                    .streamName("test_stream")
                    .sequence(
                            JournalDocumentSequence.builder()
                                    .timestamp(System.currentTimeMillis())
                                    .offset(new SequenceOffset("counter_journal_1", 1L))
                                    .build()
                    )
                    .streamVersionStart(1)
                    .streamVersionEnd(1)
                    .entry(
                            JournalDocumentEntry.builder()
                                    .id("counter_journal_1-1-0")
                                    .metadata(Metadata.nullMetadata())
                                    .streamVersion(1)
                                    .type(String.class.getName())
                                    .entry(new Document("entry", "this is the serialized entry"))
                                    .typeVersion(1)
                                    .build()
                    )
                    .build();
            journal.insertOne(collectionEntry.getDocument());

            final Entry<Document> result = underTest.readNext().await();
            assertThat(result.id()).isEqualTo("counter_journal_1-1-0");
        }

        @Test
        void readNextFail() {
            final JournalDocumentAdapter collectionEntry = JournalDocumentAdapter.builder()
                    .streamName("test_stream")
                    .sequence(
                            JournalDocumentSequence.builder()
                                    .timestamp(System.currentTimeMillis())
                                    .offset(new SequenceOffset("counter_journal_1", 1L))
                                    .build()
                    )
                    .streamVersionStart(1)
                    .streamVersionEnd(1)
                    .entry(
                            JournalDocumentEntry.builder()
                                    .id("counter_journal_1-1-0")
                                    .metadata(Metadata.nullMetadata())
                                    .streamVersion(1)
                                    .type(String.class.getName())
                                    .entry(new Document("entry", "this is the serialized entry"))
                                    .typeVersion(1)
                                    .build()
                    )
                    .build();
            final JournalDocumentAdapter collectionEntry2 = JournalDocumentAdapter.builder()
                    .streamName("test_stream")
                    .sequence(
                            JournalDocumentSequence.builder()
                                    .timestamp(System.currentTimeMillis())
                                    .offset(new SequenceOffset("counter_journal_1", 3L))
                                    .build()
                    )
                    .streamVersionStart(4)
                    .streamVersionEnd(4)
                    .entry(
                            JournalDocumentEntry.builder()
                                    .id("counter_journal_1-3-0")
                                    .metadata(Metadata.nullMetadata())
                                    .streamVersion(4)
                                    .type(String.class.getName())
                                    .entry(new Document("entry", "this is the serialized entry"))
                                    .typeVersion(1)
                                    .build()
                    )
                    .build();
            journal.insertMany(Arrays.asList(collectionEntry.getDocument(), collectionEntry2.getDocument()));

            underTest.readNext(2).await();

            final JournalDocumentAdapter collectionEntry3 = JournalDocumentAdapter.builder()
                    .streamName("test_stream")
                    .sequence(
                            JournalDocumentSequence.builder()
                                    .timestamp(System.currentTimeMillis())
                                    .offset(new SequenceOffset("counter_journal_1", 2L))
                                    .build()
                    )
                    .streamVersionStart(2)
                    .streamVersionEnd(3)
                    .entry(
                            JournalDocumentEntry.builder()
                                    .id("counter_journal_1-2-0")
                                    .metadata(Metadata.nullMetadata())
                                    .streamVersion(2)
                                    .type(String.class.getName())
                                    .entry(new Document("entry", "this is the serialized entry"))
                                    .typeVersion(1)
                                    .build()
                    )
                    .entry(
                            JournalDocumentEntry.builder()
                                    .id("counter_journal_1-2-1")
                                    .metadata(Metadata.nullMetadata())
                                    .streamVersion(1)
                                    .type(String.class.getName())
                                    .entry(new Document("entry", "this is the other serialized entry"))
                                    .typeVersion(3)
                                    .build()
                    )
                    .build();
            journal.insertOne(collectionEntry3.getDocument());

            underTest.readNext().outcome();
//            assertThat(result.id()).isEqualTo("counter_journal_1-1-0");
        }

        @Test
        void readNext1() {
        }

    }

    @Test
    void rewind() {
    }

    @Test
    void seekTo() {
    }

    @Test
    void seekTo1() {
    }
}