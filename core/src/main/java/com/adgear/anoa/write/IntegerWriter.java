package com.adgear.anoa.write;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

class IntegerWriter extends JacksonWriter<Integer> {

  @Override
  public void write(Integer integer, JsonGenerator jsonGenerator) throws IOException {
    jsonGenerator.writeNumber(integer);
  }
}
