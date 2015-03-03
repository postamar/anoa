package com.adgear.anoa.factory.util;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;

import java.io.IOException;
import java.util.function.Function;

public class ThriftWriteConsumer<T extends TBase<T, ? extends TFieldIdEnum>>
    implements WriteConsumer<T> {

  final protected TTransport tTransport;
  final public TProtocol tProtocol;


  public ThriftWriteConsumer(TTransport tTransport,
                             Function<TTransport, TProtocol> protocolFactory) {
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
  public void accept(T t) {
    try {
      t.write(tProtocol);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }
}
