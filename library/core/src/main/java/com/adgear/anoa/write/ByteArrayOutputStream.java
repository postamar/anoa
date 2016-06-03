package com.adgear.anoa.write;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;

/**
 * Unsynchronized version of java.io.ByteArrayOutputStream
 */
class ByteArrayOutputStream extends java.io.ByteArrayOutputStream {

  private int oldCapacity;

  ByteArrayOutputStream() {
    this(32768);
  }

  ByteArrayOutputStream(int size) {
    super(size);
    oldCapacity = size;
  }

  private void ensureCapacity(int minCapacity) {
    // overflow-conscious code
    if (minCapacity - buf.length > 0) {
      grow(minCapacity);
    }
  }

  private void grow(int minCapacity) {
    // overflow-conscious code
    int newCapacity = oldCapacity + buf.length;
    if (newCapacity - minCapacity < 0) {
      newCapacity = minCapacity;
    }
    if (newCapacity < 0) {
      if (minCapacity < 0) {
        // overflow
        throw new OutOfMemoryError();
      }
      newCapacity = Integer.MAX_VALUE;
    }
    oldCapacity = buf.length;
    buf = Arrays.copyOf(buf, newCapacity);
  }

  @Override
  public void write(byte[] b, int off, int len) {
    if ((off < 0) || (off > b.length) || (len < 0) ||
        ((off + len) - b.length > 0)) {
      throw new IndexOutOfBoundsException();
    }
    ensureCapacity(count + len);
    System.arraycopy(b, off, buf, count, len);
    count += len;
  }

  @Override
  public void write(int b) {
    ensureCapacity(count + 1);
    buf[count] = (byte) b;
    count += 1;
  }

  @Override
  public byte toByteArray()[] {
    return Arrays.copyOf(buf, count);
  }

  @Override
  public int size() {
    return count;
  }

  @Override
  public String toString(String charsetName) throws UnsupportedEncodingException {
    return new String(buf, 0, count, charsetName);
  }

  @Override
  public String toString() {
    return new String(buf, 0, count);
  }

  @Override
  public void reset() {
    count = 0;
  }

  @Override
  public void writeTo(OutputStream out) throws IOException {
    out.write(buf, 0, count);
  }
}
