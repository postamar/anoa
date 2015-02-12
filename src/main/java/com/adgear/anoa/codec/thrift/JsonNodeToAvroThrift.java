package com.adgear.anoa.codec.thrift;

import com.adgear.anoa.avro.ThriftDataModified;
import com.adgear.anoa.avro.decode.JsonNodeDecoder;
import com.adgear.anoa.avro.decode.ThriftDatumTextReader;
import com.adgear.anoa.codec.base.SchemalessToAvroBase;
import com.adgear.anoa.provider.Provider;
import com.fasterxml.jackson.databind.JsonNode;

import org.apache.avro.io.Decoder;
import org.apache.thrift.TBase;

/**
 * Transform Jackson <code>JsonNode</code> instances into Thrift records.
 *
 * @param <T> Type of the Thrift record to be provided by the Codec.
 * @see com.adgear.anoa.codec.Codec
 * @see com.adgear.anoa.provider.Provider
 * @see com.adgear.anoa.source.schemaless.JsonNodeSource
 * @see com.adgear.anoa.codec.schemaless.BytesToJsonNode
 * @see com.adgear.anoa.codec.schemaless.StringToJsonNode
 */
public class JsonNodeToAvroThrift<T extends TBase<T,?>> extends SchemalessToAvroBase<JsonNode,T> {

  /**
   * @param provider    An upstream Provider of JsonNode instances.
   * @param thriftClass The class object corresponding to the serialized Thrift records.
   */
  public JsonNodeToAvroThrift(Provider<JsonNode> provider, Class<T> thriftClass) {
    super(provider,
          ThriftDataModified.getModified().getSchema(thriftClass),
          new ThriftDatumTextReader<>(thriftClass));
    reader.withFieldNames();
  }

  @Override
  protected Decoder makeDecoder(JsonNode input) {
    return new JsonNodeDecoder(input);
  }
}
