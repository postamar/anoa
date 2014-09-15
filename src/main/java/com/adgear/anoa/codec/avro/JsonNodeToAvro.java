package com.adgear.anoa.codec.avro;

import com.adgear.anoa.avro.decode.JsonNodeDecoder;
import com.adgear.anoa.codec.base.SchemalessToAvroBase;
import com.adgear.anoa.provider.Provider;
import com.fasterxml.jackson.databind.JsonNode;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;

/**
 * Transform Jackson <code>JsonNode</code> instances into Avro records.
 *
 * @param <R> Type of the Avro record to be provided by the Codec.
 * @see com.adgear.anoa.codec.Codec
 * @see com.adgear.anoa.provider.avro.AvroProvider
 * @see com.adgear.anoa.source.schemaless.JsonNodeSource
 * @see com.adgear.anoa.codec.schemaless.BytesToJsonNode
 * @see com.adgear.anoa.codec.schemaless.StringToJsonNode
 */
public class JsonNodeToAvro<R extends IndexedRecord> extends SchemalessToAvroBase<JsonNode, R> {

  /**
   * Constructs a SpecificRecord Provider.
   *
   * @param recordClass class object of SpecificRecord implementation.
   */
  public JsonNodeToAvro(Provider<JsonNode> provider, Class<R> recordClass) {
    super(provider, recordClass);
    reader.withFieldNames();
  }

  /**
   * Constructs a GenericRecord Provider.
   */
  public JsonNodeToAvro(Provider<JsonNode> provider, Schema recordSchema) {
    super(provider, recordSchema);
    reader.withFieldNames();
  }

  @Override
  protected Decoder makeDecoder(JsonNode input) {
    return new JsonNodeDecoder(input);
  }
}