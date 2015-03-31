package com.adgear.anoa.read;

import com.adgear.anoa.Anoa;
import com.adgear.anoa.AnoaHandler;
import com.fasterxml.jackson.core.JsonParser;

import org.apache.thrift.TBase;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TTransport;

import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Utility class for generating functions for deserializing Thrift records. Unless specified
 * otherwise, the functions should not be deemed thread-safe.
 */
public class ThriftDecoders {

  protected ThriftDecoders() {
  }

  /** 
   * @param supplier provides the returned Thrift record instances
   * @param <T> Thrift record type 
   * @return A function which deserializes a Thrift record from its compact binary encoding
   */
  static public <T extends TBase> /*@NonNull*/ Function<byte[], T> compact(
      /*@NonNull*/ Supplier<T> supplier) {
    return fn(supplier, TCompactProtocol::new);
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param supplier provides the returned Thrift record instances
   * @param <T> Thrift record type
   * @param <M> Metadata type 
   * @return A function which deserializes a Thrift record from its compact binary encoding 
   */
  static public <T extends TBase, M>
  /*@NonNull*/ Function<Anoa<byte[], M>, Anoa<T, M>> compact(
      /*@NonNull*/ AnoaHandler<M> anoaHandler,
      /*@NonNull*/ Supplier<T> supplier) {
    return fn(anoaHandler, supplier, TCompactProtocol::new);
  }

  /**
   * @param supplier provides the returned Thrift record instances
   * @param <T> Thrift record type
   * @return A function which deserializes a Thrift record from its standard binary encoding
   */
  static public <T extends TBase> /*@NonNull*/ Function<byte[], T> binary(
      /*@NonNull*/ Supplier<T> supplier) {
    return fn(supplier, TBinaryProtocol::new);
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param supplier provides the returned Thrift record instances
   * @param <T> Thrift record type
   * @param <M> Metadata type
   * @return A function which deserializes a Thrift record from its standard binary encoding
   */
  static public <T extends TBase, M>
  /*@NonNull*/ Function<Anoa<byte[], M>, Anoa<T, M>> binary(
      /*@NonNull*/ AnoaHandler<M> anoaHandler,
      /*@NonNull*/ Supplier<T> supplier) {
    return fn(anoaHandler, supplier, TBinaryProtocol::new);
  }

  /**
   * @param supplier provides the returned Thrift record instances
   * @param <T> Thrift record type
   * @return A function which deserializes a Thrift record from its Thrift JSON encoding
   */
  static public <T extends TBase> /*@NonNull*/ Function<byte[], T> json(
      /*@NonNull*/ Supplier<T> supplier) {
    return fn(supplier, TJSONProtocol::new);
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param supplier provides the returned Thrift record instances
   * @param <T> Thrift record type
   * @param <M> Metadata type
   * @return A function which deserializes a Thrift record from its Thrift JSON encoding
   */
  static public <T extends TBase, M>
  /*@NonNull*/ Function<Anoa<byte[], M>, Anoa<T, M>> json(
      /*@NonNull*/ AnoaHandler<M> anoaHandler,
      /*@NonNull*/ Supplier<T> supplier) {
    return fn(anoaHandler, supplier, TJSONProtocol::new);
  }

  static <T extends TBase> /*@NonNull*/ Function<byte[], T> fn(
      /*@NonNull*/ Supplier<T> supplier,
      /*@NonNull*/ Function<TTransport, TProtocol> protocolFactory) {
    final TMemoryInputTransport tTransport = new TMemoryInputTransport();
    final TProtocol tProtocol = protocolFactory.apply(tTransport);
    final LookAheadIterator<T> lookAheadIterator =
        LookAheadIteratorFactory.thrift(tProtocol, supplier);
    return (byte[] bytes) -> {
      lookAheadIterator.reset(null);
      tTransport.reset(bytes);
      return lookAheadIterator.next();
    };
  }

  static <T extends TBase, M> /*@NonNull*/ Function<Anoa<byte[], M>, Anoa<T, M>> fn(
      /*@NonNull*/ AnoaHandler<M> anoaHandler,
      /*@NonNull*/ Supplier<T> supplier,
      /*@NonNull*/ Function<TTransport, TProtocol> protocolFactory) {
    final TMemoryInputTransport tTransport = new TMemoryInputTransport();
    final TProtocol tProtocol = protocolFactory.apply(tTransport);
    final LookAheadIterator<Anoa<T, M>> lookAheadIterator =
        LookAheadIteratorFactory.thrift(anoaHandler, tProtocol, supplier);
    return (Anoa<byte[], M> bytesWrapped) -> bytesWrapped.flatMap(bytes -> {
      lookAheadIterator.reset(null);
      tTransport.reset(bytesWrapped.get());
      return lookAheadIterator.next();
    });
  }

  /** 
   * @param recordClass Thrift record class object
   * @param strict enable strict type checking
   * @param <P> Jackson JsonParser type
   * @param <T> Thrift record type
   * @return A function which reads a Thrift record from a JsonParser, in its 'natural' encoding.
   */
  static public <P extends JsonParser, T extends TBase>
  /*@NonNull*/ Function<P, T> jackson(
      /*@NonNull*/ Class<T> recordClass,
      boolean strict) {
    final AbstractReader<T> reader = new ThriftReader<>(recordClass);
    return (P jp) -> reader.read(jp, strict);
  }

  /** 
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param recordClass Thrift record class object
   * @param strict enable strict type checking
   * @param <P> Jackson JsonParser type
   * @param <T> Thrift record type
   * @param <M> Metadata type
   * @return A function which reads a Thrift record from a JsonParser, in its 'natural' encoding.
   */
  static public <P extends JsonParser, T extends TBase, M>
  /*@NonNull*/ Function<Anoa<P, M>, Anoa<T, M>> jackson(
      /*@NonNull*/ AnoaHandler<M> anoaHandler,
      /*@NonNull*/ Class<T> recordClass,
      boolean strict) {
    final AbstractReader<T> reader = new ThriftReader<>(recordClass);
    return anoaHandler.functionChecked((P jp) -> (T) reader.readChecked(jp, strict));
  }
}
