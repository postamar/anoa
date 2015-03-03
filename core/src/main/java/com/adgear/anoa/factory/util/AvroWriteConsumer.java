package com.adgear.anoa.factory.util;

import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;

import java.io.IOException;
import java.io.UncheckedIOException;

public class AvroWriteConsumer<R extends IndexedRecord> implements WriteConsumer<R> {

  final public DatumWriter<R> writer;
  final protected Encoder encoder;

  public AvroWriteConsumer(DatumWriter<R> writer, Encoder encoder) {
    this.writer = writer;
    this.encoder = encoder;
  }

  @Override
  public void flush() throws IOException {
    encoder.flush();
  }

  @Override
  public void accept(R r) {
    try {
      writer.write(r, encoder);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
