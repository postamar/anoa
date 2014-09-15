package com.adgear.anoa.codec.serialized;

import com.adgear.anoa.codec.base.JsonNodeSerializerBase;
import com.adgear.anoa.provider.Provider;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;

/**
 * Transforms Jackson <code>JsonNode</code> objects into JSON serializations.
 *
 * @see com.adgear.anoa.codec.Codec
 * @see com.adgear.anoa.source.schemaless.JsonNodeSource
 * @see com.adgear.anoa.codec.schemaless.BytesToJsonNode
 * @see com.adgear.anoa.codec.schemaless.StringToJsonNode
 */
public class JsonNodeToString extends JsonNodeSerializerBase<String> {

  public JsonNodeToString(Provider<JsonNode> provider) {
    super(provider);
  }

  @Override
  protected String serialize(JsonNode input) throws IOException {
    return objectMapper.writeValueAsString(input);
  }
}
