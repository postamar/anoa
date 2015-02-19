package com.adgear.anoa.write;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

class DoubleWriter extends JacksonWriter<Double> {

  @Override
  public void write(Double aDouble, JsonGenerator jsonGenerator) throws IOException {
    jsonGenerator.writeNumber(aDouble);
  }
}
