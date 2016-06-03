package com.adgear.anoa.write;

import com.fasterxml.jackson.core.JsonGenerator;

import org.apache.thrift.TBase;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.transport.TFileTransport;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;

/**
 * Utility class for generating {@code WriteConsumer} instances to write Thrift records.
 */
final public class ThriftConsumers {

  private ThriftConsumers() {
  }

  /**
   * Write as compact binary blobs.
   *
   * @param outputStream stream to write into
   * @param <T>          Thrift record type
   */
  static public <T extends TBase> WriteConsumer<T> compact(
      OutputStream outputStream) {
    return compact(new TIOStreamTransport(new BufferedOutputStream(outputStream)));
  }

  /**
   * Write as compact binary blobs.
   *
   * @param fileName name of file to write into
   * @param <T>      Thrift record type
   */
  static public <T extends TBase> WriteConsumer<T> compact(
      String fileName) {
    try {
      return compact(new TFileTransport(fileName, false));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Write as compact binary blobs.
   *
   * @param tTransport the {@link org.apache.thrift.transport.TTransport} instance to write into
   * @param <T>        Thrift record type
   */
  static public <T extends TBase> WriteConsumer<T> compact(
      TTransport tTransport) {
    return new ThriftWriteConsumer<>(tTransport, TCompactProtocol::new);
  }

  /**
   * Write as standard binary blobs.
   *
   * @param outputStream stream to write into
   * @param <T>          Thrift record type
   */
  static public <T extends TBase> WriteConsumer<T> binary(
      OutputStream outputStream) {
    return binary(new TIOStreamTransport(new BufferedOutputStream(outputStream)));
  }

  /**
   * Write as standard binary blobs.
   *
   * @param fileName name of file to write into
   * @param <T>      Thrift record type
   */
  static public <T extends TBase> WriteConsumer<T> binary(
      String fileName) {
    try {
      return binary(new TFileTransport(fileName, false));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Write as standard binary blobs.
   *
   * @param tTransport the {@link org.apache.thrift.transport.TTransport} instance to write into
   * @param <T>        Thrift record type
   */
  static public <T extends TBase> WriteConsumer<T> binary(
      TTransport tTransport) {
    return new ThriftWriteConsumer<>(tTransport, TBinaryProtocol::new);
  }

  /**
   * Write as JSON serializations.
   *
   * @param outputStream stream to write into
   * @param <T>          Thrift record type
   */
  static public <T extends TBase> WriteConsumer<T> json(
      OutputStream outputStream) {
    return json(new TIOStreamTransport(new BufferedOutputStream(outputStream)));
  }

  /**
   * Write as Thrift JSON serializations.
   *
   * @param fileName name of file to write into
   * @param <T>      Thrift record type
   */
  static public <T extends TBase> WriteConsumer<T> json(
      String fileName) {
    try {
      return json(new TFileTransport(fileName, false));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Write as Thrift JSON serializations.
   *
   * @param tTransport the {@link org.apache.thrift.transport.TTransport} instance to write into
   * @param <T>        Thrift record type
   */
  static public <T extends TBase> WriteConsumer<T> json(
      TTransport tTransport) {
    return new ThriftWriteConsumer<>(tTransport, TJSONProtocol::new);
  }

  /**
   * Write as 'natural' compact JSON serializations using provided generator.
   *
   * @param recordClass      Thrift record class object
   * @param jacksonGenerator JsonGenerator instance to write into
   * @param <T>              Thrift record type
   */
  static public <T extends TBase> WriteConsumer<T> jackson(
      Class<T> recordClass,
      JsonGenerator jacksonGenerator) {
    return new ThriftWriter<>(recordClass).writeConsumer(jacksonGenerator);
  }

  /**
   * Write as 'natural' 'strict' JSON serializations using provided generator
   *
   * @param recordClass      Thrift record class object
   * @param jacksonGenerator JsonGenerator instance to write into
   * @param <T>              Thrift record type
   */
  static public <T extends TBase> WriteConsumer<T> jacksonStrict(
      Class<T> recordClass,
      JsonGenerator jacksonGenerator) {
    return new ThriftWriter<>(recordClass).writeConsumerStrict(jacksonGenerator);
  }
}
