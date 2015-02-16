package com.adgear.anoa.io.write.json;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

class EnumWriter extends JsonWriter<Enum> {

  @Override
  public void write(Enum anEnum, JsonGenerator jsonGenerator) throws IOException {
    jsonGenerator.writeString(anEnum.toString());
  }
}
