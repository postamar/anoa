package com.adgear.anoa.read;

import com.adgear.anoa.AnoaTypeException;
import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;
import java.nio.ByteBuffer;

class ByteBufferReader extends JacksonReader<ByteBuffer> {

  static private ByteArrayReader byteArrayReader = new ByteArrayReader();

  @Override
  public ByteBuffer read(JsonParser jp) throws IOException {
    final byte[] array = byteArrayReader.read(jp);
    return (array == null) ? null : ByteBuffer.wrap(array);
  }

  @Override
  public ByteBuffer readStrict(JsonParser jp) throws AnoaTypeException, IOException {
    final byte[] array = byteArrayReader.readStrict(jp);
    return (array == null) ? null : ByteBuffer.wrap(array);
  }
}
