package com.adgear.anoa.write;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

class FloatWriter extends JacksonWriter<Float> {

  @Override
  public void write(Float aFloat, JsonGenerator jsonGenerator) throws IOException {
    jsonGenerator.writeNumber(aFloat);
  }
}
