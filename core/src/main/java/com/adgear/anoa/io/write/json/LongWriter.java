package com.adgear.anoa.io.write.json;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

class LongWriter extends JsonWriter<Long> {

  @Override
  public void write(Long aLong, JsonGenerator jsonGenerator) throws IOException {
    jsonGenerator.writeNumber(aLong);
  }
}
