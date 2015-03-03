package com.adgear.anoa.factory;

import checkers.nullness.quals.NonNull;

import com.adgear.anoa.factory.util.ThriftReadIterator;

import org.apache.thrift.TBase;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TTransport;

import java.util.function.Function;
import java.util.function.Supplier;

public class ThriftDecoders {

  static public <T extends TBase<T,?>> @NonNull Function<byte[],T> compact(
      @NonNull Supplier<T> supplier) {
    return fn(supplier, TCompactProtocol::new);
  }

  static public <T extends TBase<T,?>> @NonNull Function<byte[],T> binary(
      @NonNull Supplier<T> supplier) {
    return fn(supplier, TBinaryProtocol::new);
  }

  static public <T extends TBase<T,?>> @NonNull Function<byte[],T> json(
      @NonNull Supplier<T> supplier) {
    return fn(supplier, TJSONProtocol::new);
  }

  static public <T extends TBase<T,?>> @NonNull Function<byte[],T> fn(
      @NonNull Supplier<T> supplier,
      @NonNull Function<TTransport, TProtocol> protocolFactory) {
    TMemoryInputTransport tTransport = new TMemoryInputTransport();
    TProtocol tProtocol = protocolFactory.apply(tTransport);
    ThriftReadIterator<T> iterator = new ThriftReadIterator<>(tTransport, tProtocol, supplier);
    return (byte[] bytes) -> {
      tTransport.reset(bytes);
      return iterator.next();
    };
  }

}
