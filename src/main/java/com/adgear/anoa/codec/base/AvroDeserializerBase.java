package com.adgear.anoa.codec.base;

import com.adgear.anoa.provider.Provider;
import com.adgear.anoa.provider.avro.AvroProvider;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 * Base class for codecs deserializing Avro records from the Avro binary format.
 *
 * @param <R> Type of the Avro record to be provided by the Codec.
 */
abstract public class AvroDeserializerBase<R>
    extends CodecBase<byte[], R, AvroDeserializerBase.Counter>
    implements AvroProvider<R> {

  final private Schema schema;
  final private DatumReader<R> reader;
  private BinaryDecoder decoder = null;

  protected AvroDeserializerBase(Provider<byte[]> provider,
                                 Schema schema,
                                 DatumReader<R> reader) {
    super(provider, Counter.class);
    this.schema = schema;
    this.reader = reader;
  }

  @Override
  public Schema getAvroSchema() {
    return schema;
  }

  @Override
  public R transform(byte[] input) {
    ByteArrayInputStream bais = new ByteArrayInputStream(input);
    decoder = DecoderFactory.get().directBinaryDecoder(bais, decoder);
    try {
      return reader.read(null, decoder);
    } catch (IOException e) {
      logger.warn(e.getMessage());
      increment(Counter.AVRO_DESERIALIZE_FAIL);
      return null;
    }
  }

  static public enum Counter {
    /**
     * Counts corrupted binary reprensentations.
     */
    AVRO_DESERIALIZE_FAIL
  }
}
