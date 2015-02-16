package com.adgear.anoa.io.write.json;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

class DoubleWriter extends JsonWriter<Double> {

  @Override
  public void write(Double aDouble, JsonGenerator jsonGenerator) throws IOException {
    jsonGenerator.writeNumber(aDouble);
  }
}
