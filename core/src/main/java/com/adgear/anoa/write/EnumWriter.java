package com.adgear.anoa.write;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

class EnumWriter extends AbstractWriter<Object> {

  @Override
  protected void writeChecked(Object anEnum, JsonGenerator jacksonGenerator) throws IOException {
    jacksonGenerator.writeString(anEnum.toString());
  }
}
