package com.adgear.anoa.write;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

class DoubleWriter extends AbstractWriter<Double> {

  @Override
  protected void writeChecked(Double aDouble, JsonGenerator jacksonGenerator) throws IOException {
    jacksonGenerator.writeNumber(aDouble);
  }
}
