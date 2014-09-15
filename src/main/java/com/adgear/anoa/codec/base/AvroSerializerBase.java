package com.adgear.anoa.codec.base;

import com.adgear.anoa.provider.Provider;
import com.adgear.anoa.provider.avro.AvroProvider;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Base class for codecs serializing Avro records into the Avro binary format.
 *
 * @param <R> Type of the Avro record provided to the Codec.
 * @see com.adgear.anoa.provider.avro.AvroProvider
 * @see com.adgear.anoa.codec.Codec
 */
abstract public class AvroSerializerBase<R extends IndexedRecord>
    extends CodecBase<R, byte[], AvroSerializerBase.Counter>
    implements AvroProvider<byte[]> {

  final private Schema schema;
  final private DatumWriter<R> writer;
  final private ByteArrayOutputStream baos;
  private BinaryEncoder encoder = null;

  protected AvroSerializerBase(Provider<R> provider,
                               Schema schema,
                               DatumWriter<R> writer) {
    super(provider, AvroSerializerBase.Counter.class);
    this.schema = schema;
    this.writer = writer;
    this.baos = new ByteArrayOutputStream();
  }

  @Override
  public Schema getAvroSchema() {
    return schema;
  }

  @Override
  public byte[] transform(R input) {
    baos.reset();
    encoder = EncoderFactory.get().directBinaryEncoder(baos, encoder);
    try {
      writer.write(input, encoder);
      encoder.flush();
      baos.flush();
    } catch (IOException e) {
      logger.warn(e.getMessage());
      increment(Counter.AVRO_SERIALIZE_FAIL);
      return null;
    }
    return baos.toByteArray();
  }

  static public enum Counter {
    /**
     * Counts failures to serialize to binary Avro representation.
     */
    AVRO_SERIALIZE_FAIL
  }

}
