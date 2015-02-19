package com.adgear.anoa.write;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

class EnumWriter extends JacksonWriter<Enum> {

  @Override
  public void write(Enum anEnum, JsonGenerator jsonGenerator) throws IOException {
    jsonGenerator.writeString(anEnum.toString());
  }
}
