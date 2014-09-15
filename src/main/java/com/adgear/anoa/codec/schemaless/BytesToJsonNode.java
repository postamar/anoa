package com.adgear.anoa.codec.schemaless;

import com.adgear.anoa.codec.base.JsonNodeDeserializerBase;
import com.adgear.anoa.provider.Provider;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;

/**
 * Transforms serialized JSON objects into Jackson <code>JsonNode</code> instances.
 *
 * @see com.adgear.anoa.codec.Codec
 * @see com.adgear.anoa.source.serialized.BytesLineSource
 * @see com.adgear.anoa.codec.schemaless.ThriftToJsonBytes
 * @see com.adgear.anoa.codec.serialized.JsonNodeToBytes
 */
public class BytesToJsonNode extends JsonNodeDeserializerBase<byte[]> {

  public BytesToJsonNode(Provider<byte[]> provider) {
    super(provider);
  }

  @Override
  protected JsonNode parse(byte[] input) throws IOException {
    return objectMapper.readTree(input);
  }

  @Override
  protected boolean isRecordEmpty(byte[] input) {
    return new String(input).trim().isEmpty();
  }
}
