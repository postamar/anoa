package com.adgear.anoa.io.write.json;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

class IntegerWriter extends JsonWriter<Integer> {

  @Override
  public void write(Integer integer, JsonGenerator jsonGenerator) throws IOException {
    jsonGenerator.writeNumber(integer);
  }
}
