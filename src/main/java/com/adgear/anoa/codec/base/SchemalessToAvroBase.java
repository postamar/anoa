package com.adgear.anoa.codec.base;

import com.adgear.anoa.avro.decode.GenericDatumTextReader;
import com.adgear.anoa.avro.decode.SpecificDatumTextReader;
import com.adgear.anoa.provider.Provider;
import com.adgear.anoa.provider.avro.AvroProvider;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.specific.SpecificData;

import java.io.IOException;

abstract public class SchemalessToAvroBase<S, R extends IndexedRecord>
    extends CounterlessCodecBase<S, R> implements AvroProvider<R> {

  final protected Schema avroSchema;
  protected GenericDatumTextReader<R> reader;

  protected SchemalessToAvroBase(Provider<S> provider, Class<R> recordClass) {
    this(provider,
         SpecificData.get().getSchema(recordClass),
         new SpecificDatumTextReader<>(recordClass));
  }

  protected SchemalessToAvroBase(Provider<S> provider, Schema recordSchema) {
    this(provider, recordSchema, new GenericDatumTextReader<R>(recordSchema));
  }

  private SchemalessToAvroBase(Provider<S> provider,
                               Schema recordSchema,
                               GenericDatumTextReader<R> reader) {
    super(provider);
    this.avroSchema = recordSchema;
    this.reader = reader.withBytesAsBase64();
  }

  abstract protected Decoder makeDecoder(S input);

  @Override
  public R transform(final S input) {
    try {
      return reader.read(null, makeDecoder(input));
    } catch (IOException e) {
      logger.warn(e.getMessage());
      return null;
    }
  }

  @Override
  public Schema getAvroSchema() {
    return avroSchema;
  }
}
