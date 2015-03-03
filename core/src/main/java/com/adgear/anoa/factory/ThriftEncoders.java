package com.adgear.anoa.factory;

import checkers.nullness.quals.NonNull;

import com.adgear.anoa.factory.util.TMemoryOutputTransport;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;

import java.util.function.Function;

public class ThriftEncoders {

  static public <T extends TBase<T,?>> @NonNull Function<T,byte[]> compact() {
    return fn(TCompactProtocol::new);
  }

  static public <T extends TBase<T,?>> @NonNull Function<T,byte[]> binary() {
    return fn(TBinaryProtocol::new);
  }

  static public <T extends TBase<T,?>> @NonNull Function<T,byte[]> json() {
    return fn(TJSONProtocol::new);
  }

  static protected <T extends TBase<T,?>> @NonNull Function<T,byte[]> fn(
      @NonNull Function<TTransport, TProtocol> protocolFactory) {
    TMemoryOutputTransport tTransport = new TMemoryOutputTransport();
    TProtocol tProtocol = protocolFactory.apply(tTransport);
    return (T t) -> {
      tTransport.baos.reset();
      try {
        t.write(tProtocol);
      } catch (TException e) {
        throw new RuntimeException(e);
      }
      return tTransport.baos.toByteArray();
    };
  }

}
