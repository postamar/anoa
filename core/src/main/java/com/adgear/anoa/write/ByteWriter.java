package com.adgear.anoa.write;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

class ByteWriter extends AbstractWriter<Byte> {

  @Override
  protected void writeChecked(Byte aByte, JsonGenerator jacksonGenerator) throws IOException {
    jacksonGenerator.writeNumber(aByte);
  }
}
