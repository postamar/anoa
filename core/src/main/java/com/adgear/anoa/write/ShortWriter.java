package com.adgear.anoa.write;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

class ShortWriter extends AbstractWriter<Short> {

  @Override
  protected void writeChecked(Short aShort, JsonGenerator jacksonGenerator) throws IOException {
    jacksonGenerator.writeNumber(aShort);
  }
}
