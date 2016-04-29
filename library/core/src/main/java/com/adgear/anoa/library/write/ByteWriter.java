package com.adgear.anoa.library.write;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

class ByteWriter extends AbstractWriter<Byte> {

  @Override
  protected void write(Byte aByte, JsonGenerator jacksonGenerator) throws IOException {
    jacksonGenerator.writeNumber(aByte);
  }
}
