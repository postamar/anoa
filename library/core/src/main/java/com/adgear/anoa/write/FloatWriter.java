package com.adgear.anoa.write;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

class FloatWriter extends AbstractWriter<Float> {

  @Override
  protected void write(Float aFloat, JsonGenerator jacksonGenerator) throws IOException {
    jacksonGenerator.writeNumber(aFloat);
  }
}
