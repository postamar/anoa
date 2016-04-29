package com.adgear.anoa.library.write;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

class ByteArrayWriter extends AbstractWriter<byte[]> {

  @Override
  protected void write(byte[] bytes, JsonGenerator jacksonGenerator) throws IOException {
    jacksonGenerator.writeBinary(bytes);
  }
}
