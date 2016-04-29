package com.adgear.anoa.library.write;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;

import java.io.IOException;
import java.util.function.Function;

class ThriftWriteConsumer<T extends TBase> implements WriteConsumer<T> {

  final TTransport tTransport;
  final TProtocol tProtocol;

  ThriftWriteConsumer(TTransport tTransport, Function<TTransport, TProtocol> protocolFactory) {
    this.tTransport = tTransport;
    this.tProtocol = protocolFactory.apply(tTransport);
  }

  @Override
  public void flush() throws IOException {
    try {
      tTransport.flush();
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void close() throws IOException {
    flush();
    tTransport.close();
  }

  @Override
  public void acceptChecked(T record) throws IOException {
    try {
      record.write(tProtocol);
    } catch (TException e) {
      throw new IOException(e);
    }
  }
}
