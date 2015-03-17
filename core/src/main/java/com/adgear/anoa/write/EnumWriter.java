package com.adgear.anoa.write;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

class EnumWriter extends AbstractWriter<Enum> {

  @Override
  protected void writeChecked(Enum anEnum, JsonGenerator jacksonGenerator) throws IOException {
    jacksonGenerator.writeString(anEnum.toString());
  }
}
