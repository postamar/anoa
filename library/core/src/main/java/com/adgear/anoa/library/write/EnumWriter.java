package com.adgear.anoa.library.write;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

class EnumWriter extends AbstractWriter<Object> {

  @Override
  protected void write(Object anEnum, JsonGenerator jacksonGenerator) throws IOException {
    jacksonGenerator.writeString(anEnum.toString());
  }
}
