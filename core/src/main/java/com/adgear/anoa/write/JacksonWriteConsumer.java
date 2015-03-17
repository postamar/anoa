package com.adgear.anoa.write;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.io.UncheckedIOException;

class JacksonWriteConsumer<T> implements WriteConsumer<T, IOException> {

  final JsonGenerator jacksonGenerator;
  final AbstractWriter<T> writer;

  JacksonWriteConsumer(JsonGenerator jacksonGenerator, AbstractWriter<T> writer) {
    this.jacksonGenerator = jacksonGenerator;
    this.writer = writer;
  }

  @Override
  public void acceptChecked(T record) throws IOException{
    writer.writeChecked(record, jacksonGenerator);
  }

  @Override
  public void accept(T record) {
    try {
      acceptChecked(record);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public void flush() throws IOException {
    jacksonGenerator.flush();
  }
}
