package com.adgear.anoa.codec.schemaless;

import com.adgear.anoa.avro.encode.SpecificDatumTextWriter;
import com.adgear.anoa.avro.encode.StringListEncoder;
import com.adgear.anoa.codec.base.AvroToSchemalessBase;
import com.adgear.anoa.provider.Provider;
import com.adgear.anoa.provider.avro.AvroProvider;

import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificData;

import java.util.List;

/**
 * Transforms Avro SpecificRecords into String lists, when possible. <p> In other words, requires a
 * flat record schema. Value ordering follows Schema field ordering.
 *
 * @param <R> Type of the Avro record to be consumed by the Codec.
 * @see com.adgear.anoa.codec.Codec
 * @see com.adgear.anoa.source.avro.AvroSource
 * @see com.adgear.anoa.codec.avro.BytesToAvroSpecific
 * @see com.adgear.anoa.codec.avro.JsonNodeToAvro
 * @see com.adgear.anoa.codec.avro.StringListToAvro
 * @see com.adgear.anoa.codec.avro.ValueToAvro
 */
public class AvroSpecificToStringList<R extends IndexedRecord>
    extends AvroToSchemalessBase<R, List<String>> {

  public AvroSpecificToStringList(AvroProvider<R> provider) {
    super(provider,
          provider.getAvroSchema(),
          new StringListEncoder(),
          new SpecificDatumTextWriter<R>(provider.getAvroSchema()));
  }

  /**
   * @param provider    To be used as upstream Provider.
   * @param recordClass The class object of the records returned by the given upstream Provider.
   */
  public AvroSpecificToStringList(Provider<R> provider, Class<R> recordClass) {
    super(provider,
          SpecificData.get().getSchema(recordClass),
          new StringListEncoder(),
          new SpecificDatumTextWriter<>(recordClass));
  }

}
