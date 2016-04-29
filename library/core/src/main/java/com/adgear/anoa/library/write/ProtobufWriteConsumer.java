package com.adgear.anoa.library.write;

import com.google.protobuf.MessageLite;

import java.io.IOException;
import java.io.OutputStream;

class ProtobufWriteConsumer<R extends MessageLite> implements WriteConsumer<R> {

  final OutputStream outputStream;

  public ProtobufWriteConsumer(OutputStream outputStream) {
    this.outputStream = outputStream;
  }

  @Override
  public void acceptChecked(R record) throws IOException {
    record.writeDelimitedTo(outputStream);
  }

  @Override
  public void flush() throws IOException {
    outputStream.flush();
  }
}
