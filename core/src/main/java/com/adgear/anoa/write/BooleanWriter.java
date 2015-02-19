package com.adgear.anoa.write;


import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

class BooleanWriter extends JacksonWriter<Boolean> {

  @Override
  public void write(Boolean aBoolean, JsonGenerator jsonGenerator) throws IOException {
    jsonGenerator.writeBoolean(aBoolean);
  }
}
