package com.adgear.anoa.source.serialized;

import com.adgear.anoa.provider.base.CounterlessProviderBase;
import com.adgear.anoa.source.Source;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * A Source for iterating over lines in a text stream, exposed as byte[].
 *
 * @see com.adgear.anoa.source.serialized.StringLineSource
 */
public class BytesLineSource extends CounterlessProviderBase<byte[]> implements Source<byte[]> {

  final private BufferedInputStream stream;
  private ByteArrayOutputStream baos = new ByteArrayOutputStream();
  private byte[] nextEntry = null;

  public BytesLineSource(InputStream in) {
    this(new BufferedInputStream(in));
  }

  public BytesLineSource(BufferedInputStream in) {
    this.stream = in;
  }

  protected void update() throws IOException {
    if (baos != null && nextEntry == null) {
      baos.reset();
      int b;
      while (nextEntry == null) {
        b = stream.read();
        if (b == -1 || b == '\n') {
          nextEntry = baos.toByteArray();
          if (b == -1) {
            baos = null;
          }
        } else {
          baos.write(b);
        }
      }
    }
  }

  @Override
  public void close() throws IOException {
    stream.close();
  }

  @Override
  protected byte[] getNext() throws IOException {
    update();
    byte[] result = nextEntry;
    nextEntry = null;
    return result;
  }

  @Override
  public boolean hasNext() {
    try {
      update();
    } catch (IOException e) {
      logger.error(e.getMessage());
      baos = null;
    }
    return (baos != null || nextEntry != null);
  }
}
