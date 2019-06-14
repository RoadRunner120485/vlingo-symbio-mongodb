package io.vlingo.symbio.store.mongodb.journal.reader;

import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
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

import java.util.List;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
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
        this.underTest = world.actorFor(type,
                MongoDBJournalReaderActor.class,
                "just_the_name", configuration
        );

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
            prepareEntry(1, 1, 1);

            final Entry<Document> result = underTest.readNext().await();
            assertThat(result.id()).isEqualTo("counter_journal_1-1-0");
        }

        @Test
        void readNextPartialGaps() throws Exception {
            prepareEntry(1, 1, 1);
            prepareEntry(3, 4, 4);

            underTest.readNext(2).await();

            prepareEntry(2, 2, 3);

            final Entry<Document> result1 = underTest.readNext().await();
            assertThat(result1.id()).isEqualTo("counter_journal_1-2-0");
            final Entry<Document> result2 = underTest.readNext().await();
            assertThat(result2.id()).isEqualTo("counter_journal_1-2-1");
            assertThat(underTest.readNext().await()).isNull();
        }

        @Test
        void readNext1() {
        }

    }

    private void prepareEntry(long offset, int streamVersionStart, int streamVersionEnd) {
        final List<JournalDocumentEntry> entries = IntStream.range(0, streamVersionEnd + 1 - streamVersionStart)
                .boxed()
                .map(i -> JournalDocumentEntry.builder()
                        .id(String.format("counter_journal_1-%s-%s", offset, i))
                        .metadata(Metadata.nullMetadata())
                        .streamVersion(i)
                        .type(String.class.getName())
                        .entry(new Document("entry", "this is the serialized entry"))
                        .typeVersion(1)
                        .build()
                )
                .collect(toList());
        final JournalDocumentAdapter collectionEntry = JournalDocumentAdapter.builder()
                .streamName("test_stream")
                .sequence(
                        JournalDocumentSequence.builder()
                                .timestamp(System.currentTimeMillis())
                                .offset(new SequenceOffset("counter_journal_1", offset))
                                .build()
                )
                .streamVersionStart(streamVersionStart)
                .streamVersionEnd(streamVersionEnd)
                .entries(entries)
                .build();
        journal.insertOne(collectionEntry.getDocument());
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