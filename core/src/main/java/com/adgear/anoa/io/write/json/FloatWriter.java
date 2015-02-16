package com.adgear.anoa.io.write.json;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

class FloatWriter extends JsonWriter<Float> {

  @Override
  public void write(Float aFloat, JsonGenerator jsonGenerator) throws IOException {
    jsonGenerator.writeNumber(aFloat);
  }
}
