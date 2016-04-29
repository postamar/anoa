package com.adgear.anoa.library.write;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;

class AvroBatchWriteConsumer<R extends IndexedRecord> implements WriteConsumer<R> {

  final DataFileWriter<R> dataFileWriter;

  AvroBatchWriteConsumer(DataFileWriter<R> dataFileWriter) {
    this.dataFileWriter = dataFileWriter;
  }

  @Override
  public void acceptChecked(R record) throws IOException {
    dataFileWriter.append(record);
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
