package com.adgear.anoa.library.write;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.nio.ByteBuffer;

class ByteBufferWriter extends AbstractWriter<ByteBuffer> {

  @Override
  protected void write(ByteBuffer bb, JsonGenerator jacksonGenerator) throws IOException {
    if (bb.hasArray() && !bb.isReadOnly()) {
      jacksonGenerator.writeBinary(bb.array(), bb.arrayOffset(), bb.remaining());
    } else {
      byte[] bytes = new byte[bb.remaining()];
      bb.get(bytes);
      jacksonGenerator.writeBinary(bytes);
    }
  }
}
