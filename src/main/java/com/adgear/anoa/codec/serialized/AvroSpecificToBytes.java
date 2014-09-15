package com.adgear.anoa.codec.serialized;

import com.adgear.anoa.codec.base.AvroSerializerBase;
import com.adgear.anoa.provider.Provider;
import com.adgear.anoa.provider.avro.AvroProvider;

import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;


/**
 * Transforms Avro SpecificRecords into Avro binary serializations.
 *
 * @see com.adgear.anoa.codec.Codec
 * @see com.adgear.anoa.provider.avro.AvroProvider
 * @see com.adgear.anoa.source.avro.AvroSource
 * @see com.adgear.anoa.codec.avro.BytesToAvroSpecific
 * @see com.adgear.anoa.codec.avro.JsonNodeToAvro
 * @see com.adgear.anoa.codec.avro.StringListToAvro
 * @see com.adgear.anoa.codec.avro.ValueToAvro
 */
public class AvroSpecificToBytes<R extends SpecificRecord> extends AvroSerializerBase<R> {

  public AvroSpecificToBytes(AvroProvider<R> provider) {
    super(provider,
          provider.getAvroSchema(),
          new SpecificDatumWriter<R>(provider.getAvroSchema()));
  }

  /**
   * @param provider    To be used as upstream Provider.
   * @param recordClass The class object of the records returned by the given upstream Provider.
   */
  public AvroSpecificToBytes(Provider<R> provider, Class<R> recordClass) {
    super(provider,
          SpecificData.get().getSchema(recordClass),
          new SpecificDatumWriter<>(recordClass));
  }

}
