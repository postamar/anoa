package com.adgear.anoa.write;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

class StringWriter extends AbstractWriter<CharSequence> {

  @Override
  protected void write(CharSequence charSequence, JsonGenerator jacksonGenerator)
      throws IOException {
    jacksonGenerator.writeString(charSequence.toString());
  }
}
