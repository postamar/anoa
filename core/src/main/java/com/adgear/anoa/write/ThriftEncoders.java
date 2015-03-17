package com.adgear.anoa.write;

import checkers.nullness.quals.NonNull;

import com.adgear.anoa.Anoa;
import com.adgear.anoa.AnoaFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;

import java.util.function.Function;
import java.util.function.Supplier;

public class ThriftEncoders {

  static public <T extends TBase<T, ? extends TFieldIdEnum>>
  @NonNull Function<T, byte[]> compact() {
    return fn(TCompactProtocol::new);
  }

  static public <T extends TBase<T, ? extends TFieldIdEnum>, M>
  @NonNull Function<Anoa<T, M>, Anoa<byte[], M>> compact(@NonNull AnoaFactory<M> anoaFactory) {
    return fn(anoaFactory, TCompactProtocol::new);
  }

  static public <T extends TBase<T, ? extends TFieldIdEnum>>
  @NonNull Function<T, byte[]> binary() {
    return fn(TBinaryProtocol::new);
  }

  static public <T extends TBase<T, ? extends TFieldIdEnum>, M>
  @NonNull Function<Anoa<T, M>, Anoa<byte[], M>> binary(@NonNull AnoaFactory<M> anoaFactory) {
    return fn(anoaFactory, TBinaryProtocol::new);
  }

  static public <T extends TBase<T, ? extends TFieldIdEnum>>
  @NonNull Function<T, byte[]> json() {
    return fn(TJSONProtocol::new);
  }

  static public <T extends TBase<T, ? extends TFieldIdEnum>, M>
  @NonNull Function<Anoa<T, M>, Anoa<byte[], M>> json(@NonNull AnoaFactory<M> anoaFactory) {
    return fn(anoaFactory, TJSONProtocol::new);
  }

  static protected <T extends TBase<T, ? extends TFieldIdEnum>> @NonNull Function<T, byte[]> fn(
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

  static protected <T extends TBase<T, ? extends TFieldIdEnum>, M>
  @NonNull Function<Anoa<T, M>, Anoa<byte[], M>> fn(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull Function<TTransport, TProtocol> protocolFactory) {
    TMemoryOutputTransport tTransport = new TMemoryOutputTransport();
    TProtocol tProtocol = protocolFactory.apply(tTransport);
    return anoaFactory.functionChecked((T record) -> {
      tTransport.baos.reset();
      record.write(tProtocol);
      return tTransport.baos.toByteArray();
    });
  }

  static public <F extends TFieldIdEnum, T extends TBase<T, F>, G extends JsonGenerator>
  @NonNull Function<T, G> jackson(
      @NonNull Supplier<G> supplier,
      @NonNull Class<T> recordClass) {
    ThriftWriter<F, T> thriftWriter = new ThriftWriter<>(recordClass);
    return (T record) -> {
      G jg = supplier.get();
      thriftWriter.write(record, jg);
      return jg;
    };
  }

  static public <F extends TFieldIdEnum, T extends TBase<T, F>, G extends JsonGenerator, M>
  @NonNull Function<Anoa<T, M>, Anoa<G, M>> jackson(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull Supplier<G> supplier,
      @NonNull Class<T> recordClass) {
    ThriftWriter<F, T> thriftWriter = new ThriftWriter<>(recordClass);
    return anoaFactory.functionChecked((T record) -> {
      G jg = supplier.get();
      thriftWriter.writeChecked(record, jg);
      return jg;
    });
  }
}
