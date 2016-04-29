package com.adgear.anoa.write;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

class DoubleWriter extends AbstractWriter<Double> {

  @Override
  protected void write(Double aDouble, JsonGenerator jacksonGenerator) throws IOException {
    jacksonGenerator.writeNumber(aDouble);
  }
}
