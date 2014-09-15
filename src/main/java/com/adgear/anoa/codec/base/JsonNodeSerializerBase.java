package com.adgear.anoa.codec.base;

import com.adgear.anoa.provider.Provider;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * Base class for serializing Json objects.
 *
 * @param <OUT> Type of the record to be provided by the Codec.
 */
abstract public class JsonNodeSerializerBase<OUT>
    extends CodecBase<JsonNode, OUT, JsonNodeSerializerBase.Counter> {

  static final protected ObjectMapper objectMapper = new ObjectMapper();

  protected JsonNodeSerializerBase(Provider<JsonNode> provider) {
    super(provider, Counter.class);
  }

  @Override
  public OUT transform(JsonNode input) {
    try {
      return serialize(input);
    } catch (IOException e) {
      increment(Counter.JSON_SERIALIZATION_FAIL);
      logger.warn(e.getMessage());
      return null;
    }
  }

  abstract protected OUT serialize(JsonNode input) throws IOException;

  static public enum Counter {
    /**
     * Counts the number of times the Jackson ObjectMapper instance failed to serialize a record.
     */
    JSON_SERIALIZATION_FAIL
  }


}
