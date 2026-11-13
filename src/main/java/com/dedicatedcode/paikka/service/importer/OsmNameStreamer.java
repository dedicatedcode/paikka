package com.dedicatedcode.paikka.service.importer;
import de.topobyte.osm4j.core.model.iface.OsmEntity;
import de.topobyte.osm4j.core.model.iface.OsmTag;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class OsmNameStreamer implements AutoCloseable {
    private final BufferedWriter writer;
    private boolean closed;

    public OsmNameStreamer(String outputPath) throws IOException {
        // Explicit UTF-8 encoder that replaces malformed input instead of throwing: a single
        // unencodable character (e.g. an unpaired surrogate from a broken PBF string) must not
        // kill the encoder and thereby silently truncate the rest of the names file.
        Path path = Paths.get(outputPath);
        if (path.getParent() != null) {
            Files.createDirectories(path.getParent());
        }
        OutputStream out = Files.newOutputStream(path, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
        CharsetEncoder encoder = StandardCharsets.UTF_8.newEncoder()
                .onMalformedInput(CodingErrorAction.REPLACE)
                .onUnmappableCharacter(CodingErrorAction.REPLACE);
        this.writer = new BufferedWriter(new OutputStreamWriter(out, encoder));
    }

    public void processEntity(OsmEntity entity, String type) throws IOException {
        long id = entity.getId();
        int numTags = entity.getNumberOfTags();

        StringBuilder jsonBuilder = new StringBuilder();
        jsonBuilder.append("{");
        boolean hasNames = false;

        for (int i = 0; i < numTags; i++) {
            OsmTag tag = entity.getTag(i);
            String key = tag.getKey();

            if ("name".equals(key) || (key != null && key.startsWith("name:"))) {
                String value = tag.getValue();
                if (value != null && !value.isBlank()) {
                    if (hasNames) {
                        jsonBuilder.append(",");
                    }
                    hasNames = true;

                    // Build standard JSON key-value pairs
                    jsonBuilder.append("\"").append(escapeJson(key)).append("\":")
                            .append("\"").append(escapeJson(value)).append("\"");
                }
            }
        }
        jsonBuilder.append("}");

        if (hasNames) {
            String jsonString = jsonBuilder.toString();

            // Escape the finished JSON string specifically for PG Text-Mode COPY rules
            String postgresSafeJson = escapeSQL(jsonString);

            // Writes exactly 3 columns matching your schema: osm_id, osm_type, all_names
            this.writer.write(id + "\t" + type + "\t" + postgresSafeJson + "\n");
        }
    }

    /**
     * Step 1: Encodes values to safely fit inside a JSON string property. Unpaired UTF-16
     * surrogates are replaced with the replacement character so the value is always encodable.
     */
    private String escapeJson(String value) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            switch (ch) {
                case '"': sb.append("\\\""); break;
                case '\\': sb.append("\\\\"); break;
                case '\b': sb.append("\\b"); break;
                case '\f': sb.append("\\f"); break;
                case '\n': sb.append("\\n"); break;
                case '\r': sb.append("\\r"); break;
                case '\t': sb.append("\\t"); break;
                default:
                    if (Character.isSurrogate(ch)) {
                        if (Character.isHighSurrogate(ch) && i + 1 < value.length()
                                && Character.isLowSurrogate(value.charAt(i + 1))) {
                            sb.append(ch).append(value.charAt(i + 1));
                            i++;
                        } else {
                            sb.append('\uFFFD');
                        }
                    } else if (ch < ' ') {
                        String ss = Integer.toHexString(ch);
                        sb.append("\\u");
                        sb.repeat("0", 4 - ss.length());
                        sb.append(ss.toUpperCase());
                    } else {
                        sb.append(ch);
                    }
            }
        }
        return sb.toString();
    }

    private String escapeSQL(String text) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < text.length(); i++) {
            char ch = text.charAt(i);
            switch (ch) {
                case '\\' -> sb.append("\\\\");
                case '\t' -> sb.append("\\t");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                default -> sb.append(ch);
            }
        }
        return sb.toString();
    }


    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        writer.flush();
        writer.close();
    }
}
