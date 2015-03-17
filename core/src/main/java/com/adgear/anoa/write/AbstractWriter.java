package com.adgear.anoa.write;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.io.UncheckedIOException;

abstract class AbstractWriter<T> {

  abstract void writeChecked(T in, JsonGenerator jacksonGenerator) throws IOException;

  void write(T t, JsonGenerator jacksonGenerator) {
    try {
      writeChecked(t, jacksonGenerator);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
