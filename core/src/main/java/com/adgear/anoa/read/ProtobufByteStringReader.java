package com.adgear.anoa.read;

import com.google.protobuf.ByteString;

import com.adgear.anoa.AnoaJacksonTypeException;
import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;

class ProtobufByteStringReader extends AbstractReader<ByteString> {

  static private ByteArrayReader byteArrayReader = new ByteArrayReader();

  @Override
  protected ByteString read(JsonParser jacksonParser) throws IOException {
    final byte[] array = byteArrayReader.read(jacksonParser);
    return (array == null) ? ByteString.EMPTY : ByteString.copyFrom(array);
  }

  @Override
  protected ByteString readStrict(JsonParser jacksonParser)
      throws AnoaJacksonTypeException, IOException {
    final byte[] array = byteArrayReader.readStrict(jacksonParser);
    return (array == null) ? ByteString.EMPTY : ByteString.copyFrom(array);
  }
}
