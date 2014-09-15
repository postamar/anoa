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
public class JsonNodeToBytes extends JsonNodeSerializerBase<byte[]> {

  public JsonNodeToBytes(Provider<JsonNode> provider) {
    super(provider);
  }

  @Override
  protected byte[] serialize(JsonNode input) throws IOException {
    return objectMapper.writeValueAsBytes(input);
  }
}
