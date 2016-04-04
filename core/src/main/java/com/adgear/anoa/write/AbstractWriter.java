package com.adgear.anoa.write;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

abstract class AbstractWriter<T> {

  abstract void write(T in, JsonGenerator jacksonGenerator) throws IOException;

  void writeStrict(T in, JsonGenerator jacksonGenerator) throws IOException {
    write(in, jacksonGenerator);
  }

}
