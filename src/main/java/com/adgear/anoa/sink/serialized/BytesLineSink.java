package com.adgear.anoa.sink.serialized;

import com.adgear.anoa.provider.Provider;
import com.adgear.anoa.sink.Sink;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Collects byte array records into a newline-separated file.
 *
 * @see com.adgear.anoa.sink.Sink
 * @see com.adgear.anoa.source.serialized.BytesLineSource
 * @see com.adgear.anoa.codec.schemaless.ThriftToJsonBytes
 * @see com.adgear.anoa.codec.serialized.JsonNodeToBytes
 * @see com.adgear.anoa.codec.serialized.ValueToJsonBytes
 */
public class BytesLineSink implements Sink<byte[], BytesLineSink> {

  final private BufferedOutputStream stream;

  public BytesLineSink(BufferedOutputStream out) {
    this.stream = out;
  }

  /**
   * @param out A stream which will be wrapped in a {@link java.io.BufferedOutputStream}.
   */
  public BytesLineSink(OutputStream out) {
    this(new BufferedOutputStream(out));
  }

  @Override
  public BytesLineSink append(byte[] record) throws IOException {
    if (record != null) {
      stream.write(record);
      stream.write('\n');
    }
    return this;
  }

  @Override
  public BytesLineSink appendAll(Provider<byte[]> provider) throws IOException {
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
