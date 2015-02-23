package com.adgear.anoa.write;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.nio.ByteBuffer;

class ByteBufferWriter extends JacksonWriter<ByteBuffer> {

  @Override
  public void write(ByteBuffer byteBuffer, JsonGenerator jsonGenerator) throws IOException {
    jsonGenerator.writeBinary(byteBuffer.array());
  }
}
