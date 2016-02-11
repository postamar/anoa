package com.adgear.anoa.write;

import com.adgear.anoa.Anoa;
import com.adgear.anoa.AnoaHandler;
import com.fasterxml.jackson.core.JsonGenerator;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;

import java.util.function.Function;
import java.util.function.Supplier;


/**
 * Utility class for generating functions for serializing Thrift records. Unless specified
 * otherwise, the functions should not be deemed thread-safe.
 */
public class ThriftEncoders {

  protected ThriftEncoders() {
  }

  /**
   * @param <T> Thrift record type
   * @return A function for serializing Thrift records as compact binary blobs.
   */
  static public <T extends TBase> Function<T, byte[]> compact() {
    return fn(TCompactProtocol::new);
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param <T>         Thrift record type
   * @param <M>         Metadata type
   * @return A function for serializing Thrift records as compact binary blobs.
   */
  static public <T extends TBase, M> Function<Anoa<T, M>, Anoa<byte[], M>> compact(
      AnoaHandler<M> anoaHandler) {
    return fn(anoaHandler, TCompactProtocol::new);
  }

  /**
   * @param <T> Thrift record type
   * @return A function for serializing Thrift records as compact binary blobs.
   */
  static public <T extends TBase> Function<T, byte[]> binary() {
    return fn(TBinaryProtocol::new);
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param <T>         Thrift record type
   * @param <M>         Metadata type
   * @return A function for serializing Thrift records as standard binary blobs.
   */
  static public <T extends TBase, M> Function<Anoa<T, M>, Anoa<byte[], M>> binary(
      AnoaHandler<M> anoaHandler) {
    return fn(anoaHandler, TBinaryProtocol::new);
  }

  /**
   * @param <T> Thrift record type
   * @return A function for serializing Thrift records in Thrift JSON format.
   */
  static public <T extends TBase> Function<T, byte[]> json() {
    return fn(TJSONProtocol::new);
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param <T>         Thrift record type
   * @param <M>         Metadata type
   * @return A function for serializing Thrift records in Thrift JSON format.
   */
  static public <T extends TBase, M> Function<Anoa<T, M>, Anoa<byte[], M>> json(
      AnoaHandler<M> anoaHandler) {
    return fn(anoaHandler, TJSONProtocol::new);
  }

  static <T extends TBase> Function<T, byte[]> fn(
      Function<TTransport, TProtocol> protocolFactory) {
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

  static <T extends TBase, M> Function<Anoa<T, M>, Anoa<byte[], M>> fn(
      AnoaHandler<M> anoaHandler,
      Function<TTransport, TProtocol> protocolFactory) {
    TMemoryOutputTransport tTransport = new TMemoryOutputTransport();
    TProtocol tProtocol = protocolFactory.apply(tTransport);
    return anoaHandler.functionChecked((T record) -> {
      tTransport.baos.reset();
      record.write(tProtocol);
      return tTransport.baos.toByteArray();
    });
  }

  /**
   * @param recordClass Thrift record class object
   * @param supplier    called for each new record serialization
   * @param <T>         Thrift record type
   * @param <G>         JsonGenerator type
   * @return A function which calls the supplier for a JsonGenerator object and writes the record
   * into it.
   */
  static public <T extends TBase, G extends JsonGenerator> Function<T, G> jackson(
      Class<T> recordClass,
      Supplier<G> supplier) {
    return JacksonUtils.encoder(new ThriftWriter<>(recordClass), supplier);
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param recordClass Thrift record class object
   * @param supplier    called for each new record serialization
   * @param <T>         Thrift record type
   * @param <G>         JsonGenerator type
   * @param <M>         Metadata type
   * @return A function which calls the supplier for a JsonGenerator object and writes the record
   * into it.
   */
  static public <T extends TBase, G extends JsonGenerator, M>
  Function<Anoa<T, M>, Anoa<G, M>> jackson(
      AnoaHandler<M> anoaHandler,
      Class<T> recordClass,
      Supplier<G> supplier) {
    return JacksonUtils.encoder(anoaHandler, new ThriftWriter<>(recordClass), supplier);
  }
}
