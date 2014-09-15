package com.adgear.anoa.avro.encode;

import org.apache.avro.io.Encoder;

/**
 * Extension to {@link Encoder} with a method for returning the encoded value, because the derived
 * classes are used in {@link java.util.Iterator} implementations instead of writing to a stream.
 *
 * @param <S> Type of the record to be built.
 * @see com.adgear.anoa.codec.base.AvroToSchemalessBase
 * @see org.apache.avro.io.Encoder
 */
abstract public class EncoderBase<S> extends Encoder {

  /**
   * Builder method called by {@link com.adgear.anoa.codec.base.AvroToSchemalessBase}.
   *
   * @return A completed record.
   */
  abstract public S build();
}
