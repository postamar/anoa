package com.adgear.anoa.write;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

class StringWriter extends JacksonWriter<CharSequence> {

  @Override
  public void write(CharSequence charSequence, JsonGenerator jsonGenerator) throws IOException {
    jsonGenerator.writeString(charSequence.toString());
  }
}
