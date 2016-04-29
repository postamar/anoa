package com.adgear.anoa.library.write;

import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;

import java.io.IOException;

class AvroWriteConsumer<R extends IndexedRecord> implements WriteConsumer<R> {

  final DatumWriter<R> writer;
  final Encoder encoder;

  AvroWriteConsumer(DatumWriter<R> writer, Encoder encoder) {
    this.writer = writer;
    this.encoder = encoder;
  }

  @Override
  public void acceptChecked(R record) throws IOException {
    writer.write(record, encoder);
  }

  @Override
  public void flush() throws IOException {
    encoder.flush();
  }
}
