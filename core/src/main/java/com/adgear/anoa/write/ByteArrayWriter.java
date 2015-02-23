package com.adgear.anoa.write;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

class ByteArrayWriter extends JacksonWriter<byte[]> {

  @Override
  protected void write(byte[] bytes, JsonGenerator jg) throws IOException {
    jg.writeBinary(bytes);
  }
}
