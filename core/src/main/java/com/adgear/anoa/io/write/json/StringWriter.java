package com.adgear.anoa.io.write.json;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

class StringWriter extends JsonWriter<CharSequence> {

  @Override
  public void write(CharSequence charSequence, JsonGenerator jsonGenerator) throws IOException {
    jsonGenerator.writeString(charSequence.toString());
  }
}
