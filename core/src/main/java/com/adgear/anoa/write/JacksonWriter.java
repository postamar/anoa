package com.adgear.anoa.write;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.BiConsumer;

abstract class JacksonWriter<T> implements BiConsumer<T, JsonGenerator> {

  abstract protected void write(T in, JsonGenerator jp) throws IOException;

  @Override
  public void accept(T t, JsonGenerator jg) throws UncheckedIOException {
    try {
      write(t, jg);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
