package com.adgear.anoa.write;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.io.UncheckedIOException;

class AvroBatchWriteConsumer<R extends IndexedRecord> implements WriteConsumer<R, IOException> {

  final DataFileWriter<R> dataFileWriter;

  AvroBatchWriteConsumer(DataFileWriter<R> dataFileWriter) {
    this.dataFileWriter = dataFileWriter;
  }

  @Override
  public void acceptChecked(R record) throws IOException {
    dataFileWriter.append(record);
  }

  @Override
  public void accept(R record) {
    try {
      acceptChecked(record);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public void flush() throws IOException {
    dataFileWriter.flush();
  }

  @Override
  public void close() throws IOException {
    flush();
    dataFileWriter.close();
  }
}
