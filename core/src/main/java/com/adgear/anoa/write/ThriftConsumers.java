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
public class ThriftConsumers {

  /**
   * Write as compact binary blobs
   *
   * @param outputStream stream to write into
   * @param <T> Thrift record type
   */
  static public <T extends TBase> /*@NonNull*/ WriteConsumer<T> compact(
      /*@NonNull*/ OutputStream outputStream) {
    return compact(new TIOStreamTransport(new BufferedOutputStream(outputStream)));
  }

  /**
   * Write as compact binary blobs
   *
   * @param fileName name of file to write into
   * @param <T> Thrift record type
   */
  static public <T extends TBase> /*@NonNull*/ WriteConsumer<T> compact(
      /*@NonNull*/ String fileName) {
    try {
      return compact(new TFileTransport(fileName, false));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Write as compact binary blobs
   *
   * @param tTransport the {@link org.apache.thrift.transport.TTransport} instance to write into
   * @param <T> Thrift record type
   */
  static public <T extends TBase> /*@NonNull*/ WriteConsumer<T> compact(
      /*@NonNull*/ TTransport tTransport) {
    return new ThriftWriteConsumer<>(tTransport, TCompactProtocol::new);
  }

  /**
   * Write as standard binary blobs
   *
   * @param outputStream stream to write into
   * @param <T> Thrift record type
   */
  static public <T extends TBase> /*@NonNull*/ WriteConsumer<T> binary(
      /*@NonNull*/ OutputStream outputStream) {
    return binary(new TIOStreamTransport(new BufferedOutputStream(outputStream)));
  }

  /**
   * Write as standard binary blobs
   *
   * @param fileName name of file to write into
   * @param <T> Thrift record type
   */
  static public <T extends TBase> /*@NonNull*/ WriteConsumer<T> binary(
      /*@NonNull*/ String fileName) {
    try {
      return binary(new TFileTransport(fileName, false));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Write as standard binary blobs
   *
   * @param tTransport the {@link org.apache.thrift.transport.TTransport} instance to write into
   * @param <T> Thrift record type
   */
  static public <T extends TBase> /*@NonNull*/ WriteConsumer<T> binary(
      /*@NonNull*/ TTransport tTransport) {
    return new ThriftWriteConsumer<>(tTransport, TBinaryProtocol::new);
  }

  /**
   * Write as JSON serializations
   *
   * @param outputStream stream to write into
   * @param <T> Thrift record type
   */
  static public <T extends TBase> /*@NonNull*/ WriteConsumer<T> json(
      /*@NonNull*/ OutputStream outputStream) {
    return json(new TIOStreamTransport(new BufferedOutputStream(outputStream)));
  }

  /**
   * Write as Thrift JSON serializations
   *
   * @param fileName name of file to write into
   * @param <T> Thrift record type
   */
  static public <T extends TBase> /*@NonNull*/ WriteConsumer<T> json(
      /*@NonNull*/ String fileName) {
    try {
      return json(new TFileTransport(fileName, false));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Write as Thrift JSON serializations
   *
   * @param tTransport the {@link org.apache.thrift.transport.TTransport} instance to write into
   * @param <T> Thrift record type
   */
  static public <T extends TBase> /*@NonNull*/ WriteConsumer<T> json(
      /*@NonNull*/ TTransport tTransport) {
    return new ThriftWriteConsumer<>(tTransport, TJSONProtocol::new);
  }

  /**
   * Write as 'natural' JSON serializations using provided generator
   *
   * @param recordClass Thrift record class object
   * @param jacksonGenerator JsonGenerator instance to write into
   * @param <T> Thrift record type
   */
  static public <T extends TBase> /*@NonNull*/ WriteConsumer<T> jackson(
      /*@NonNull*/ Class<T> recordClass,
      /*@NonNull*/ JsonGenerator jacksonGenerator) {
    return new JacksonWriteConsumer<>(jacksonGenerator, new ThriftWriter<>(recordClass));
  }
}
