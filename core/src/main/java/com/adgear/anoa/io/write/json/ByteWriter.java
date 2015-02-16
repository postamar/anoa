package com.adgear.anoa.io.write.json;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

class ByteWriter extends JsonWriter<Byte> {

  @Override
  public void write(Byte aByte, JsonGenerator jsonGenerator) throws IOException {
    jsonGenerator.writeNumber(aByte);
  }
}
