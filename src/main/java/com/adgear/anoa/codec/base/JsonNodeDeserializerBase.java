package com.adgear.anoa.codec.base;

import com.adgear.anoa.provider.Provider;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * Base class for deserializing JSON objects.
 *
 * @param <IN> Type of the serialized records consumed from the upstream Provider.
 */
abstract public class JsonNodeDeserializerBase<IN>
    extends CodecBase<IN, JsonNode, JsonNodeDeserializerBase.Counter> {

  static final protected ObjectMapper objectMapper = new ObjectMapper();

  protected JsonNodeDeserializerBase(Provider<IN> provider) {
    super(provider, Counter.class);
  }

  @Override
  public JsonNode transform(IN input) {
    if (isRecordEmpty(input)) {
      increment(Counter.EMPTY_RECORD);
      return null;
    }
    try {
      return parse(input);
    } catch (IOException e) {
      increment(Counter.JSON_DESERIALIZE_FAIL);
      logger.warn(e.getMessage());
      return null;
    }
  }

  abstract protected boolean isRecordEmpty(IN input);

  abstract protected JsonNode parse(IN input) throws IOException;

  static public enum Counter {
    /**
     * Counts the number of times the Jackson ObjectMapper failed to deserialize an input record.
     */
    JSON_DESERIALIZE_FAIL,

    /**
     * Counts the number of times the input records was empty.
     */
    EMPTY_RECORD
  }

}
