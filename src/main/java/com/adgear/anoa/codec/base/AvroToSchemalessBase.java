package com.adgear.anoa.codec.base;

import com.adgear.anoa.avro.encode.EncoderBase;
import com.adgear.anoa.avro.encode.GenericDatumTextWriter;
import com.adgear.anoa.provider.Provider;
import com.adgear.anoa.provider.avro.AvroProvider;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;

/**
 * Base class for codecs serializing Avro records into schemaless representations such as JSON.
 *
 * @param <R> Type of the Avro record provided to the Codec.
 * @param <S> Type of the record to be provided by the Codec.
 */
abstract public class AvroToSchemalessBase<R extends IndexedRecord, S>
    extends CodecBase<R, S, AvroToSchemalessBase.Counter>
    implements AvroProvider<S> {

  final protected Schema schema;
  protected GenericDatumTextWriter<R> writer;
  protected EncoderBase<S> encoder;

  protected AvroToSchemalessBase(Provider<R> provider,
                                 Schema schema,
                                 EncoderBase<S> encoder,
                                 GenericDatumTextWriter<R> writer) {
    super(provider, Counter.class);
    this.schema = schema;
    this.encoder = encoder;
    this.writer = writer.withEnumsAsString().withBytesAsBase64();
  }

  @Override
  public Schema getAvroSchema() {
    return schema;
  }

  @Override
  public S transform(final R input) {
    try {
      writer.write(input, encoder);
    } catch (IOException e) {
      logger.warn(e.getMessage());
      increment(Counter.AVRO_SERIALIZE_FAIL);
      return null;
    }
    return encoder.build();
  }

  static public enum Counter {
    /**
     * Counts failures to serialize to binary Avro representation.
     */
    AVRO_SERIALIZE_FAIL
  }
}
