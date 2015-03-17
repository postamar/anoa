package com.adgear.anoa.read;

import com.adgear.anoa.AnoaTypeException;
import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;
import java.nio.ByteBuffer;

class ByteBufferReader extends AbstractReader<ByteBuffer> {

  static private ByteArrayReader byteArrayReader = new ByteArrayReader();

  @Override
  protected ByteBuffer read(JsonParser jacksonParser) throws IOException {
    final byte[] array = byteArrayReader.read(jacksonParser);
    return (array == null) ? null : ByteBuffer.wrap(array);
  }

  @Override
  protected ByteBuffer readStrict(JsonParser jacksonParser) throws AnoaTypeException, IOException {
    final byte[] array = byteArrayReader.readStrict(jacksonParser);
    return (array == null) ? null : ByteBuffer.wrap(array);
  }
}
