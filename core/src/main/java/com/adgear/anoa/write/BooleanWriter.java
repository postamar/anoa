package com.adgear.anoa.write;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

class BooleanWriter extends AbstractWriter<Boolean> {

  @Override
  protected void writeChecked(Boolean aBoolean, JsonGenerator jacksonGenerator) throws IOException {
    jacksonGenerator.writeBoolean(aBoolean);
  }
}
