package com.adgear.anoa.write;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

class JacksonWriteConsumer<T> implements WriteConsumer<T> {

  final JsonGenerator jacksonGenerator;
  final AbstractWriter<T> writer;

  JacksonWriteConsumer(JsonGenerator jacksonGenerator, AbstractWriter<T> writer) {
    this.jacksonGenerator = jacksonGenerator;
    this.writer = writer;
  }

  @Override
  public void acceptChecked(T record) throws IOException {
    writer.writeChecked(record, jacksonGenerator);
  }

  @Override
  public void flush() throws IOException {
    jacksonGenerator.flush();
  }
}
