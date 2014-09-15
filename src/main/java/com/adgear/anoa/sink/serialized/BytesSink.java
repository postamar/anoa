package com.adgear.anoa.sink.serialized;

import com.adgear.anoa.provider.Provider;
import com.adgear.anoa.sink.Sink;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Collects byte array records into a file.
 *
 * @see com.adgear.anoa.sink.Sink
 * @see com.adgear.anoa.source.serialized.BytesLineSource
 * @see com.adgear.anoa.codec.schemaless.ThriftToJsonBytes
 * @see com.adgear.anoa.codec.serialized.AvroGenericToBytes
 * @see com.adgear.anoa.codec.serialized.AvroSpecificToBytes
 * @see com.adgear.anoa.codec.serialized.JsonNodeToBytes
 * @see com.adgear.anoa.codec.serialized.ThriftToCompactBytes
 * @see com.adgear.anoa.codec.serialized.ValueToBytes
 * @see com.adgear.anoa.codec.serialized.ValueToJsonBytes
 */
public class BytesSink implements Sink<byte[], BytesSink> {

  final private BufferedOutputStream stream;

  public BytesSink(BufferedOutputStream out) {
    this.stream = out;
  }

  /**
   * @param out A stream which will be wrapped in a {@link java.io.BufferedOutputStream}.
   */
  public BytesSink(OutputStream out) {
    this(new BufferedOutputStream(out));
  }

  @Override
  public BytesSink append(byte[] record) throws IOException {
    if (record != null) {
      stream.write(record);
    }
    return this;
  }

  @Override
  public BytesSink appendAll(Provider<byte[]> provider) throws IOException {
    for (byte[] element : provider) {
      append(element);
    }
    flush();
    return this;
  }

  @Override
  public void close() throws IOException {
    stream.close();
  }

  @Override
  public void flush() throws IOException {
    stream.flush();
  }
}
