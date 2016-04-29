package com.adgear.anoa.library.write;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

class BooleanWriter extends AbstractWriter<Boolean> {

  @Override
  protected void write(Boolean aBoolean, JsonGenerator jacksonGenerator) throws IOException {
    jacksonGenerator.writeBoolean(aBoolean);
  }
}
