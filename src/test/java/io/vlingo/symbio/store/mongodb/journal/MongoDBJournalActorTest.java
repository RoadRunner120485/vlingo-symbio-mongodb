package io.vlingo.symbio.store.mongodb.journal;

import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import io.vlingo.actors.World;
import io.vlingo.actors.testkit.TestUntil;
import io.vlingo.common.Outcome;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.Source;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.StorageException;
import io.vlingo.symbio.store.journal.Journal;
import io.vlingo.symbio.store.journal.JournalListener;
import io.vlingo.symbio.store.journal.JournalReader;
import io.vlingo.symbio.store.journal.StreamReader;
import io.vlingo.symbio.store.mongodb.Configuration;
import io.vlingo.symbio.store.mongodb.journal.reader.CounterCreated;
import io.vlingo.symbio.store.mongodb.journal.reader.CounterIncremented;
import lombok.val;
import org.bson.Document;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@DisplayName("MongoDBJournalActor")
@ExtendWith(EmbeddedMongoDbExtension.class)
@ExtendWith(VlingoExtension.class)
class MongoDBJournalActorTest {

    private Journal<Document> underTest;
    private JournalListener<Document> mockJournalListener;

    public MongoDBJournalActorTest(MongoClient mongo, World world) {
        val configuration = new Configuration(
                "counter_journal_1",
//        NoopConfigurationInterest, // You will need to create your own ConfigurationInterest
                mongo,
                "vlingo",
                WriteConcern.ACKNOWLEDGED
        );
        mockJournalListener = mock(JournalListener.class);
        underTest = Journal.using(world.stage(), MongoDBJournalActor.class, mockJournalListener, configuration);
    }

    @Nested
    @DisplayName("when invoking append")
    class VerifyAppend {

        @Test
        @DisplayName("should succeed for new stream")
        void appendNewStream() {
            val until = TestUntil.happenings(1);
            doAnswer(any -> until.happened()).when(mockJournalListener).appended(any());
            final UUID streamName = UUID.randomUUID();
            val resultInterestMock = mock(Journal.AppendResultInterest.class);

            underTest.append(streamName.toString(), 0, new CounterCreated(0), resultInterestMock, null);
            until.completesWithin(1000L);

            verify(mockJournalListener).appended(any());
        }

        @Test
        @DisplayName("should succeed for existing stream")
        @SuppressWarnings("unchecked")
        void appendExistingStream() {
            val until = TestUntil.happenings(2);
            doAnswer(any -> until.happened()).when(mockJournalListener).appended(any());
            val streamName = UUID.randomUUID().toString();
            val resultInterestMock = mock(Journal.AppendResultInterest.class);

            underTest.append(streamName, 0, new CounterCreated(0), mock(Journal.AppendResultInterest.class), null);
            underTest.append(streamName, 1, new CounterIncremented(1), resultInterestMock, null);
            until.completesWithin(1000L);

            final ArgumentCaptor<Outcome<StorageException, Result>> captor = ArgumentCaptor.forClass((Class) Outcome.class);
            verify(resultInterestMock).appendResultedIn(captor.capture(), eq(streamName), eq(1), isA(CounterIncremented.class), eq(Optional.empty()), isNull());
            verify(mockJournalListener, times(2)).appended(any());

            assertThat(captor.getValue().get()).isEqualTo(Result.Success);
        }

        @Test
        @DisplayName("should fail for existing stream version")
        @SuppressWarnings("unchecked")
        void appendSameStreamTwice() {
            val until = TestUntil.happenings(2);
            doAnswer(any -> until.happened()).when(mockJournalListener).appended(any());
            val streamName = UUID.randomUUID().toString();
            val resultInterestMock = mock(Journal.AppendResultInterest.class);

            underTest.append(streamName, 0, new CounterCreated(0), mock(Journal.AppendResultInterest.class), null);
            underTest.append(streamName, 0, new CounterCreated(0), resultInterestMock, null);
            until.completesWithin(2000L);

            verify(mockJournalListener).appended(any());
            final ArgumentCaptor<Outcome<StorageException, Result>> captor = ArgumentCaptor.forClass((Class) Outcome.class);
            verify(resultInterestMock).appendResultedIn(captor.capture(), eq(streamName), eq(0), isA(CounterCreated.class), eq(Optional.empty()), isNull());

            assertThatThrownBy(() -> captor.getValue().get())
                    .isInstanceOf(StorageException.class)
                    .hasMessageContaining("StreamVersion 0 already exits in Stream " + streamName);
        }
    }


    @Test
    @DisplayName("when invoking appendWith should succeed with snapshot")
    @SuppressWarnings("unchecked")
    void appendWith() {
        val until = TestUntil.happenings(1);
        doAnswer(any -> until.happened()).when(mockJournalListener).appendedWith(any(), any());
        val streamName = UUID.randomUUID().toString();
        val resultInterestMock = mock(Journal.AppendResultInterest.class);

        underTest.appendWith(streamName, 1, new CounterCreated(0), 10, resultInterestMock, null);
        until.completesWithin(1000L);

        final ArgumentCaptor<Outcome<StorageException, Result>> captor = ArgumentCaptor.forClass((Class) Outcome.class);
        verify(resultInterestMock).appendResultedIn(captor.capture(), eq(streamName), eq(1), isA(CounterCreated.class), eq(Optional.of(10)), isNull());
        assertThat(captor.getValue().get()).isEqualTo(Result.Success);
        verify(mockJournalListener).appendedWith(any(), any());
    }

    @Test
    @DisplayName("when invoking appendAll should succeed")
    @SuppressWarnings("unchecked")
    void appendAll() {
        val until = TestUntil.happenings(1);
        doAnswer(any -> until.happened()).when(mockJournalListener).appendedAll(any());
        val streamName = UUID.randomUUID().toString();
        val resultInterestMock = mock(Journal.AppendResultInterest.class);

        underTest.appendAll(streamName, 0, Arrays.asList(new CounterCreated(0), new CounterIncremented(0)), resultInterestMock, null);
        until.completesWithin(1000L);

        final ArgumentCaptor<Outcome<StorageException, Result>> resultCaptor = ArgumentCaptor.forClass((Class) Outcome.class);
        final ArgumentCaptor<List<Source<Document>>> listCaptor = ArgumentCaptor.forClass((Class) List.class);
        verify(resultInterestMock).appendAllResultedIn(resultCaptor.capture(), eq(streamName), eq(0), listCaptor.capture(), eq(Optional.empty()), isNull());
        verify(mockJournalListener).appendedAll(any());
        assertThat(resultCaptor.getValue().get()).isEqualTo(Result.Success);
        assertThat(listCaptor.getValue()).hasSize(2);
    }

    @Test
    @DisplayName("when invoking appendAllWith should succeed with snapshot")
    @SuppressWarnings("unchecked")
    void appendAllWith() {
        val until = TestUntil.happenings(1);
        doAnswer(any -> until.happened()).when(mockJournalListener).appendedAllWith(anyList(), any());
        val streamName = UUID.randomUUID().toString();
        val resultInterestMock = mock(Journal.AppendResultInterest.class);

        underTest.appendAllWith(streamName, 1, Arrays.asList(new CounterCreated(0), new CounterIncremented(0)), 10, resultInterestMock, null);
        until.completesWithin(1000L);

        final ArgumentCaptor<Outcome<StorageException, Result>> resultCaptor = ArgumentCaptor.forClass((Class) Outcome.class);
        final ArgumentCaptor<List<Source<Document>>> listCaptor = ArgumentCaptor.forClass((Class) List.class);
        verify(resultInterestMock).appendAllResultedIn(resultCaptor.capture(), eq(streamName), eq(1), listCaptor.capture(), eq(Optional.of(10)), isNull());
        assertThat(resultCaptor.getValue().get()).isEqualTo(Result.Success);
        assertThat(listCaptor.getValue()).hasSize(2);
        verify(mockJournalListener).appendedAllWith(anyList(), any());
    }

    @Nested
    @DisplayName("when invoking journalReader")
    class VerifyJournalReader {
        @Test
        @DisplayName("should create a JournalReader Actor")
        void createJournalReader() {

            final JournalReader<Entry<?>> reader = underTest.journalReader("reader").await();
            assertThat(reader).isNotNull();
        }

        @Test
        @DisplayName("should return same JournalReader when passing same name")
        void returnCachedJournalReader() {

            final JournalReader<Entry<?>> reader1 = underTest.journalReader("reader").await();
            final JournalReader<Entry<?>> reader2 = underTest.journalReader("reader").await();
            assertThat(reader1).isSameAs(reader2);
        }

    }

    @Nested
    @DisplayName("when invoking streamReader")
    class VerifyStreamReader {
        @Test
        @DisplayName("should create a StreamReader Actor")
        void createStreamReader() {

            final StreamReader<?> reader = underTest.streamReader("reader").await();
            assertThat(reader).isNotNull();
        }

        @Test
        @DisplayName("should return same StreamReader when passing same name")
        void returnCachedStreamReader() {

            final StreamReader<?> reader1 = underTest.streamReader("reader").await();
            final StreamReader<?> reader2 = underTest.streamReader("reader").await();
            assertThat(reader1).isSameAs(reader2);
        }
    }
}
