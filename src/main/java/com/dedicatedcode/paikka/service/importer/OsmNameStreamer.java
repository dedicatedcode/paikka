package com.dedicatedcode.paikka.service.importer;
import de.topobyte.osm4j.core.model.iface.OsmEntity;
import de.topobyte.osm4j.core.model.iface.OsmTag;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class OsmNameStreamer implements AutoCloseable {
    private final BufferedWriter writer;

    public OsmNameStreamer(String outputPath) throws IOException {
        this.writer = new BufferedWriter(new FileWriter(outputPath));
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
            String postgresSafeJson = escapeForPostgresCopy(jsonString);

            // Writes exactly 3 columns matching your schema: osm_id, osm_type, all_names
            this.writer.write(id + "\t" + type + "\t" + postgresSafeJson + "\n");
        }
    }

    /**
     * Step 1: Encodes values to safely fit inside a JSON string property
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
                    if (ch < ' ') {
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

    /**
     * Step 2: Escapes control characters so Postgres COPY doesn't misinterpret them
     */
    private String escapeForPostgresCopy(String text) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < text.length(); i++) {
            char ch = text.charAt(i);
            switch (ch) {
                case '\\': sb.append("\\\\"); break; // Crucial for nested JSON backslashes
                case '\t': sb.append("\\t"); break;
                case '\n': sb.append("\\n"); break;
                case '\r': sb.append("\\r"); break;
                default: sb.append(ch);
            }
        }
        return sb.toString();
    }

    @Override
    public void close() throws IOException {
        writer.flush();
        writer.close();
    }
}