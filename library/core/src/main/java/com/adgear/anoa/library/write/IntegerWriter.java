package com.adgear.anoa.library.write;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

class IntegerWriter extends AbstractWriter<Integer> {

  @Override
  protected void write(Integer integer, JsonGenerator jacksonGenerator) throws IOException {
    jacksonGenerator.writeNumber(integer);
  }
}
