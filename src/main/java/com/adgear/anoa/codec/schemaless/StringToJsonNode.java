package com.adgear.anoa.codec.schemaless;

import com.adgear.anoa.codec.base.JsonNodeDeserializerBase;
import com.adgear.anoa.provider.Provider;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;

/**
 * Transforms serialized JSON objects into Jackson <code>JsonNode</code> instances.
 *
 * @see com.adgear.anoa.codec.Codec
 * @see com.adgear.anoa.source.serialized.StringLineSource
 * @see com.adgear.anoa.codec.serialized.JsonNodeToString
 */
public class StringToJsonNode extends JsonNodeDeserializerBase<String> {

  public StringToJsonNode(Provider<String> provider) {
    super(provider);
  }

  @Override
  protected JsonNode parse(String input) throws IOException {
    return objectMapper.readTree(input);
  }

  @Override
  protected boolean isRecordEmpty(String input) {
    return input.trim().isEmpty();
  }
}
