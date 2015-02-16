package com.adgear.anoa.io.write.json;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

class ShortWriter extends JsonWriter<Short> {

  @Override
  public void write(Short aShort, JsonGenerator jsonGenerator) throws IOException {
    jsonGenerator.writeNumber(aShort);
  }
}
