package com.adgear.anoa.codec.schemaless;

import com.adgear.anoa.avro.encode.SpecificDatumTextWriter;
import com.adgear.anoa.avro.encode.ValueEncoder;
import com.adgear.anoa.codec.base.AvroToSchemalessBase;
import com.adgear.anoa.provider.Provider;
import com.adgear.anoa.provider.avro.AvroProvider;

import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificData;
import org.msgpack.type.Value;

/**
 * Transforms Avro SpecificRecords into MessagePack <code>Value</code> instances.
 *
 * @param <R> Type of the Avro record to be consumed by the Codec.
 * @see com.adgear.anoa.codec.Codec
 * @see com.adgear.anoa.source.avro.AvroSource
 * @see com.adgear.anoa.codec.avro.BytesToAvroSpecific
 * @see com.adgear.anoa.codec.avro.JsonNodeToAvro
 * @see com.adgear.anoa.codec.avro.StringListToAvro
 * @see com.adgear.anoa.codec.avro.ValueToAvro
 */
public class AvroSpecificToValue<R extends IndexedRecord> extends AvroToSchemalessBase<R, Value> {

  public AvroSpecificToValue(AvroProvider<R> provider) {
    super(provider,
          provider.getAvroSchema(),
          new ValueEncoder(),
          new SpecificDatumTextWriter<R>(provider.getAvroSchema()));
    writer.withFieldNames();
  }

  /**
   * @param provider    To be used as upstream Provider.
   * @param recordClass The class object of the records returned by the given upstream Provider.
   */
  public AvroSpecificToValue(Provider<R> provider, Class<R> recordClass) {
    super(provider,
          SpecificData.get().getSchema(recordClass),
          new ValueEncoder(),
          new SpecificDatumTextWriter<R>(recordClass));
    writer.withFieldNames();
  }

}
