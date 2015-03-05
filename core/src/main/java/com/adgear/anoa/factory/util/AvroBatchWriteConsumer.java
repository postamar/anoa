package com.adgear.anoa.factory.util;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.io.UncheckedIOException;

public class AvroBatchWriteConsumer<R extends IndexedRecord> implements WriteConsumer<R> {

  final public DataFileWriter<R> dataFileWriter;

  public AvroBatchWriteConsumer(DataFileWriter<R> dataFileWriter) {
    this.dataFileWriter = dataFileWriter;
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

  @Override
  public void accept(R r) {
    try {
      dataFileWriter.append(r);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
