package com.adgear.anoa.read;

import checkers.nullness.quals.NonNull;

import com.adgear.anoa.Anoa;
import com.adgear.anoa.AnoaFactory;
import com.fasterxml.jackson.core.JsonParser;

import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TTransport;

import java.util.function.Function;
import java.util.function.Supplier;

public class ThriftDecoders {

  static public <T extends TBase> @NonNull Function<byte[], T> compact(
      @NonNull Supplier<T> supplier) {
    return fn(supplier, TCompactProtocol::new);
  }

  static public <T extends TBase, M>
  @NonNull Function<Anoa<byte[], M>, Anoa<T, M>> compact(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull Supplier<T> supplier) {
    return fn(anoaFactory, supplier, TCompactProtocol::new);
  }

  static public <T extends TBase> @NonNull Function<byte[], T> binary(
      @NonNull Supplier<T> supplier) {
    return fn(supplier, TBinaryProtocol::new);
  }

  static public <T extends TBase, M>
  @NonNull Function<Anoa<byte[], M>, Anoa<T, M>> binary(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull Supplier<T> supplier) {
    return fn(anoaFactory, supplier, TBinaryProtocol::new);
  }

  static public <T extends TBase> @NonNull Function<byte[], T> json(
      @NonNull Supplier<T> supplier) {
    return fn(supplier, TJSONProtocol::new);
  }

  static public <T extends TBase, M>
  @NonNull Function<Anoa<byte[], M>, Anoa<T, M>> json(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull Supplier<T> supplier) {
    return fn(anoaFactory, supplier, TJSONProtocol::new);
  }

  static public <T extends TBase> @NonNull Function<byte[], T> fn(
      @NonNull Supplier<T> supplier,
      @NonNull Function<TTransport, TProtocol> protocolFactory) {
    final TMemoryInputTransport tTransport = new TMemoryInputTransport();
    final TProtocol tProtocol = protocolFactory.apply(tTransport);
    final ReadIterator<T> readIterator = ReadIteratorUtils.thrift(tProtocol, supplier);
    return (byte[] bytes) -> {
      tTransport.reset(bytes);
      return readIterator.hasNext() ? readIterator.next() : null;
    };
  }

  static public <T extends TBase, M> @NonNull Function<Anoa<byte[], M>, Anoa<T, M>> fn(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull Supplier<T> supplier,
      @NonNull Function<TTransport, TProtocol> protocolFactory) {
    final TMemoryInputTransport tTransport = new TMemoryInputTransport();
    final TProtocol tProtocol = protocolFactory.apply(tTransport);
    final ReadIterator<Anoa<T, M>> readIterator = ReadIteratorUtils.thrift(anoaFactory,
                                                                           tProtocol,
                                                                           supplier);
    return (Anoa<byte[], M> bytesWrapped) -> {
      if (bytesWrapped.isPresent()) {
        readIterator.reset();
        tTransport.reset(bytesWrapped.get());
        return readIterator.next();
      } else {
        return new Anoa<>(bytesWrapped.meta());
      }
    };
  }

  static public <P extends JsonParser, F extends TFieldIdEnum, T extends TBase<?, F>>
  @NonNull Function<P, T> jackson(
      @NonNull Class<T> recordClass,
      boolean strict) {
    final ThriftReader<F, T> reader = new ThriftReader<>(recordClass);
    return (P jp) -> reader.read(jp, strict);
  }

  static public <P extends JsonParser, F extends TFieldIdEnum, T extends TBase<?, F>, M>
  @NonNull Function<Anoa<P, M>, Anoa<T, M>> jackson(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull Class<T> recordClass,
      boolean strict) {
    final ThriftReader<F, T> reader = new ThriftReader<>(recordClass);
    return anoaFactory.functionChecked((P jp) -> reader.readChecked(jp, strict));
  }
}
