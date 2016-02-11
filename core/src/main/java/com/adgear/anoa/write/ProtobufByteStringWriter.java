package com.adgear.anoa.write;

import com.google.protobuf.ByteString;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

class ProtobufByteStringWriter extends AbstractWriter<ByteString> {

  static private ByteBufferWriter byteBufferWriter = new ByteBufferWriter();

  @Override
  void writeChecked(ByteString byteString, JsonGenerator jacksonGenerator)
      throws IOException {
    byteBufferWriter.writeChecked(byteString.asReadOnlyByteBuffer(), jacksonGenerator);
  }
}
