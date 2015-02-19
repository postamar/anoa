package com.adgear.anoa.write;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

class LongWriter extends JacksonWriter<Long> {

  @Override
  public void write(Long aLong, JsonGenerator jsonGenerator) throws IOException {
    jsonGenerator.writeNumber(aLong);
  }
}
