package com.adgear.anoa.write;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.nio.ByteBuffer;

class ByteBufferWriter extends AbstractWriter<ByteBuffer> {

  @Override
  protected void write(ByteBuffer bb, JsonGenerator jacksonGenerator)
      throws IOException {
    jacksonGenerator.writeBinary(bb.array(), bb.arrayOffset(), bb.remaining());
  }
}
