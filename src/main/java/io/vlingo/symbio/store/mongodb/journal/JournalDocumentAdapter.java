package io.vlingo.symbio.store.mongodb.journal;

import com.google.gson.Gson;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.Metadata;
import io.vlingo.symbio.State;
import io.vlingo.symbio.store.mongodb.journal.reader.SequenceOffset;
import lombok.Builder;
import lombok.NonNull;
import lombok.Singular;
import org.bson.Document;

import java.util.List;
import java.util.Optional;

import static java.util.stream.Collectors.toList;

public class JournalDocumentAdapter {

    private static final Gson GSON = new Gson();

    private final Document document;

    public static Document queryFor(SequenceOffset offset) {
        return queryFor(offset.getId(), offset.getOffset());
    }

    public static Document queryFor(String sequence, Long offset) {
        return new Document("sequence.id", sequence)
                .append("sequence.offset", offset);
    }

    public JournalDocumentAdapter(@NonNull Document document) {
        this.document = document;
    }

    @Builder
    public JournalDocumentAdapter(@NonNull String streamName, @NonNull JournalDocumentSequence sequence, int streamVersionStart, int streamVersionEnd, @Singular @NonNull List<JournalDocumentEntry> entries, JournalDocumentState state) {
        this.document = new Document();
        document.append("streamName", streamName);
        document.append("sequence", sequence.document());
        document.append("streamVersionStart", streamVersionStart);
        document.append("streamVersionEnd", streamVersionEnd);
        document.append("entries", entries.stream().map(JournalDocumentEntry::document).collect(toList()));
        document.append("hasSnapShot", state != null);
        if (state != null) {
            document.append("state", state.document());
        }
    }

    public String getStreamName() {
        return document.getString("streamName");
    }

    public JournalDocumentSequence getSequence() {
        return new JournalDocumentSequence(document.get("sequence", Document.class));
    }

    public Integer getStreamVersionStart() {
        return document.getInteger("streamVersionStart");
    }

    public Integer getStreamVersionEnd() {
        return document.getInteger("streamVersionEnd");
    }

    public List<JournalDocumentEntry> getEntries() {
        return document.getList("entries", Document.class).stream()
                .map(doc -> new JournalDocumentEntry(doc))
                .collect(toList());
    }

    public Boolean hasSnapshot() {
        return document.getBoolean("hasSnapShot");
    }

    public Optional<JournalDocumentState> getState() {
        return hasSnapshot() ? Optional.of(document.get("state", Document.class)).map(JournalDocumentState::new) : Optional.empty();
    }

    public Document getDocument() {
        return document;
    }

    public static class JournalDocumentSequence {
        private final Document document;

        private JournalDocumentSequence(Document document) {
            this.document = document;
        }

        private JournalDocumentSequence(long timestamp, String id, long offset) {
            this.document = new Document();
            document.append("id", id);
            document.append("offset", offset);
            document.append("timestamp", timestamp);
        }

        @Builder
        public static JournalDocumentSequence fromSequenceOffset(long timestamp, SequenceOffset offset) {
            return new JournalDocumentSequence(timestamp, offset.getId(), offset.getOffset());
        }

        public Long getTimestamp() {
            return document.getLong("timestamp");
        }

        public String getId() {
            return document.getString("id");
        }

        public Long getOffset() {
            return document.getLong("offset");
        }

        Document document() {
            return document;
        }
    }

    public static class JournalDocumentState {
        private final Document document;

        private JournalDocumentState(Document document) {
            this.document = document;
        }

        @Builder
        private JournalDocumentState(Document state, String type, int dataVersion, int typeVersion, Metadata metadata) {
            this.document = new Document();
            this.document.append("state", state);
            this.document.append("type", type);
            this.document.append("dataVersion", dataVersion);
            this.document.append("typeVersion", typeVersion);
            if (!metadata.isEmpty()) {
                this.document.append("metadata", GSON.toJson(metadata));
            }
        }

        public static JournalDocumentState fromState(State<? extends Document> state) {
            return new JournalDocumentState(state.data, state.type, state.dataVersion, state.typeVersion, state.metadata);
        }

        @SuppressWarnings("unchecked")
        public Document getState() {
            return this.document.get("state", Document.class);
        }

        public String getType() {
            return this.document.getString("type");
        }

        public Integer getDataVersion() {
            return this.document.getInteger("dataVersion");
        }

        public Integer getTypeVersion() {
            return this.document.getInteger("typeVersion");
        }

        public Metadata getMetadata() {
            if (!this.document.containsKey("metadata")) {
                return Metadata.nullMetadata();
            } else {
                return GSON.fromJson(this.document.getString("metadata"), Metadata.class);
            }
        }

        Document document() {
            return document;
        }


    }

    public static class JournalDocumentEntry {
        private final Document document;

        private JournalDocumentEntry(Document document) {
            this.document = document;
        }

        @Builder
        private JournalDocumentEntry(@NonNull String id, Document entry, String type, int streamVersion, int typeVersion, Metadata metadata) {
            this.document = new Document();
            this.document.append("_id", id);
            this.document.append("entry", entry);
            this.document.append("type", type);
            this.document.append("streamVersion", streamVersion);
            this.document.append("typeVersion", typeVersion);
            if (!metadata.isEmpty()) {
                this.document.append("metadata", GSON.toJson(metadata));
            }
        }

        public static JournalDocumentEntry fromEntry(Entry<? extends Document> entry, int streamVersion) {
            return new JournalDocumentEntry(entry.id(), entry.entryData(), entry.type(), streamVersion, entry.typeVersion(), entry.metadata());
        }

        public String getId() {
            return this.document.getString("_id");
        }

        public Document getEntry() {
            return this.document.get("entry", Document.class);
        }

        public String getType() {
            return this.document.getString("type");
        }

        public Integer getStreamVersion() {
            return this.document.getInteger("streamVersion");
        }

        public Integer getTypeVersion() {
            return this.document.getInteger("typeVersion");
        }

        public Metadata getMetadata() {
            if (!this.document.containsKey("metadata")) {
                return Metadata.nullMetadata();
            } else {
                return GSON.fromJson(this.document.getString("metadata"), Metadata.class);
            }
        }

        Document document() {
            return document;
        }
    }


}
