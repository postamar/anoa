package com.adgear.anoa.io.write.json;


import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

class BooleanWriter extends JsonWriter<Boolean> {

  @Override
  public void write(Boolean aBoolean, JsonGenerator jsonGenerator) throws IOException {
    jsonGenerator.writeBoolean(aBoolean);
  }
}
