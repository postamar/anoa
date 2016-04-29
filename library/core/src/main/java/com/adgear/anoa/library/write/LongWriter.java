package com.adgear.anoa.library.write;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

class LongWriter extends AbstractWriter<Long> {

  @Override
  protected void write(Long aLong, JsonGenerator jacksonGenerator) throws IOException {
    jacksonGenerator.writeNumber(aLong);
  }
}
