package com.adgear.anoa.factory.util;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;

import java.util.function.Supplier;

public class ThriftReadIterator<T extends TBase<T,? extends TFieldIdEnum>>
    implements ReadIterator<T> {

  final public TTransport tTransport;
  final public TProtocol tProtocol;
  final public Supplier<T> tSupplier;

  public ThriftReadIterator(TTransport tTransport, TProtocol tProtocol, Supplier<T> tSupplier) {
    this.tTransport = tTransport;
    this.tProtocol = tProtocol;
    this.tSupplier = tSupplier;
  }

  @Override
  public boolean hasNext() {
    return tTransport.isOpen();
  }

  @Override
  public T next() {
    final T result = tSupplier.get();
    try {
      result.read(tProtocol);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
    return result;
  }
}
