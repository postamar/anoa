package com.adgear.anoa.write;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

class ByteArrayWriter extends AbstractWriter<byte[]> {

  @Override
  protected void writeChecked(byte[] bytes, JsonGenerator jacksonGenerator) throws IOException {
    jacksonGenerator.writeBinary(bytes);
  }
}
