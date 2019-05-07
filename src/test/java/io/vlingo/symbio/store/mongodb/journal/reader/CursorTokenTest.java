package io.vlingo.symbio.store.mongodb.journal.reader;


import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static java.util.stream.LongStream.range;
import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("CursorToken should")
class CursorTokenTest {

    @Nested
    @DisplayName("return a list of gaps")
    class FindGaps {

        @Test
        @DisplayName("containing all missing SequencePointers for a known sequence ")
        void verifyNoGaps() {
            final SequenceOffset pointer = new SequenceOffset("sequence", 5L);
            final CursorToken token = CursorToken.init(pointer, 0, 1);

            final Set<SequenceOffset> gaps = token.findGaps(pointer.next().next());

            assertThat(gaps).hasSize(1);
            assertThat(gaps).containsExactly(new SequenceOffset("sequence", 6L));
        }

        @Test
        @DisplayName("containing all sequence pointers starting with 1 for an unknown sequence")
        void verifyGaps() {
            final SequenceOffset unknownPointer = new SequenceOffset("sequence", 9L);
            final CursorToken token = CursorToken.beginning();

            final Set<SequenceOffset> gaps = token.findGaps(unknownPointer);

            assertThat(gaps).hasSize(8);
            range(1, 9)
                    .mapToObj(idx -> new SequenceOffset("sequence", idx))
                    .forEach(pointer -> assertThat(gaps).contains(pointer));
        }

    }
}