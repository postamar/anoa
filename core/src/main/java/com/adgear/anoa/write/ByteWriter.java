package com.adgear.anoa.write;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

class ByteWriter extends JacksonWriter<Byte> {

  @Override
  public void write(Byte aByte, JsonGenerator jsonGenerator) throws IOException {
    jsonGenerator.writeNumber(aByte);
  }
}
