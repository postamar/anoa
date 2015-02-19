package com.adgear.anoa.write;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

class ShortWriter extends JacksonWriter<Short> {

  @Override
  public void write(Short aShort, JsonGenerator jsonGenerator) throws IOException {
    jsonGenerator.writeNumber(aShort);
  }
}
