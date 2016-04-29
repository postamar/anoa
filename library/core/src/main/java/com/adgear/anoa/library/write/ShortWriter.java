package com.adgear.anoa.library.write;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

class ShortWriter extends AbstractWriter<Short> {

  @Override
  protected void write(Short aShort, JsonGenerator jacksonGenerator) throws IOException {
    jacksonGenerator.writeNumber(aShort);
  }
}
