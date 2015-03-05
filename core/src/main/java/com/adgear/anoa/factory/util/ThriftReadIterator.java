package com.adgear.anoa.factory.util;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.util.function.Supplier;

public class ThriftReadIterator<T extends TBase<T, ? extends TFieldIdEnum>>
    extends AbstractReadIterator<T> {

  final public TTransport tTransport;
  final public TProtocol tProtocol;
  final public Supplier<T> tSupplier;

  public ThriftReadIterator(TTransport tTransport, TProtocol tProtocol, Supplier<T> tSupplier) {
    super(() -> !tTransport.isOpen());
    this.tTransport = tTransport;
    this.tProtocol = tProtocol;
    this.tSupplier = tSupplier;
  }

  @Override
  protected T doNext() {
    final T result = tSupplier.get();
    try {
      result.read(tProtocol);
    } catch (TTransportException e) {
      if (TTransportException.END_OF_FILE == e.getType()) {
        declareNoNext();
        return null;
      } else {
        throw new RuntimeException(e);
      }
    } catch (TException e) {
      throw new RuntimeException(e);
    }
    return result;
  }
}
