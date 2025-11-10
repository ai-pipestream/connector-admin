package ai.pipestream.connector.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;

/**
 * Connector metadata structure stored as JSON in database.
 * <p>
 * Contains S3 configuration, limits, and default metadata for documents.
 */
public class ConnectorMetadata {

    private String s3Bucket = "";
    private String s3BasePath = "";
    private long maxFileSize = 0;
    private long rateLimitPerMinute = 0;
    private Map<String, String> defaultMetadata = new HashMap<>();

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public ConnectorMetadata() {}

    // Getters and setters
    public String getS3Bucket() { return s3Bucket; }
    public void setS3Bucket(String s3Bucket) { this.s3Bucket = s3Bucket; }

    public String getS3BasePath() { return s3BasePath; }
    public void setS3BasePath(String s3BasePath) { this.s3BasePath = s3BasePath; }

    public long getMaxFileSize() { return maxFileSize; }
    public void setMaxFileSize(long maxFileSize) { this.maxFileSize = maxFileSize; }

    public long getRateLimitPerMinute() { return rateLimitPerMinute; }
    public void setRateLimitPerMinute(long rateLimitPerMinute) { this.rateLimitPerMinute = rateLimitPerMinute; }

    public Map<String, String> getDefaultMetadata() { return defaultMetadata; }
    public void setDefaultMetadata(Map<String, String> defaultMetadata) { this.defaultMetadata = defaultMetadata; }

    /**
     * Serializes this metadata instance to a JSON string.
     *
     * Side effects:
     * - None.
     *
     * @return JSON representation suitable for database storage.
     * @throws RuntimeException if serialization fails; wraps {@code JsonProcessingException}.
     */
    public String toJson() {
        try {
            return MAPPER.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize connector metadata", e);
        }
    }

    /**
     * Deserializes a JSON string into a `ConnectorMetadata` instance.
     *
     * Reactive semantics:
     * - Not reactive; simple in-memory conversion using Jackson.
     *
     * Side effects:
     * - None.
     *
     * @param json JSON representation as stored in the database; accepts null, empty, or "{}" to mean default metadata.
     * @return Parsed `ConnectorMetadata` instance; returns a new default instance when input is null/empty/"{}".
     * @throws RuntimeException if deserialization fails; wraps {@code JsonProcessingException}.
     */
    public static ConnectorMetadata fromJson(String json) {
        if (json == null || json.isEmpty() || json.equals("{}")) {
            return new ConnectorMetadata();
        }
        try {
            return MAPPER.readValue(json, ConnectorMetadata.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to deserialize connector metadata", e);
        }
    }
}
