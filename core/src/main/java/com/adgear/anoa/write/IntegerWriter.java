package com.adgear.anoa.write;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

class IntegerWriter extends AbstractWriter<Integer> {

  @Override
  protected void writeChecked(Integer integer, JsonGenerator jacksonGenerator) throws IOException {
    jacksonGenerator.writeNumber(integer);
  }
}
