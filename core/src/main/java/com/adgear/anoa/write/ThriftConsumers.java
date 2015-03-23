package com.adgear.anoa.write;

import checkers.nullness.quals.NonNull;

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

public class ThriftConsumers {

  static public <T extends TBase> @NonNull WriteConsumer<T> compact(
      @NonNull OutputStream outputStream) {
    return compact(new TIOStreamTransport(new BufferedOutputStream(outputStream)));
  }

  static public <T extends TBase> @NonNull WriteConsumer<T> compact(
      @NonNull String fileName,
      @NonNull boolean readOnly) {
    try {
      return compact(new TFileTransport(fileName, readOnly));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public <T extends TBase> @NonNull WriteConsumer<T> compact(
      @NonNull TTransport tTransport) {
    return new ThriftWriteConsumer<>(tTransport, TCompactProtocol::new);
  }

  static public <T extends TBase> @NonNull WriteConsumer<T> binary(
      @NonNull OutputStream outputStream) {
    return binary(new TIOStreamTransport(new BufferedOutputStream(outputStream)));
  }

  static public <T extends TBase> @NonNull WriteConsumer<T> binary(
      @NonNull String fileName,
      @NonNull boolean readOnly) {
    try {
      return binary(new TFileTransport(fileName, readOnly));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public <T extends TBase> @NonNull WriteConsumer<T> binary(
      @NonNull TTransport tTransport) {
    return new ThriftWriteConsumer<>(tTransport, TBinaryProtocol::new);
  }

  static public <T extends TBase> @NonNull WriteConsumer<T> json(
      @NonNull OutputStream outputStream) {
    return json(new TIOStreamTransport(new BufferedOutputStream(outputStream)));
  }

  static public <T extends TBase> @NonNull WriteConsumer<T> json(
      @NonNull String fileName,
      @NonNull boolean readOnly) {
    try {
      return json(new TFileTransport(fileName, readOnly));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public <T extends TBase> @NonNull WriteConsumer<T> json(
      @NonNull TTransport tTransport) {
    return new ThriftWriteConsumer<>(tTransport, TJSONProtocol::new);
  }

  static public <T extends TBase> @NonNull WriteConsumer<T> jackson(
      @NonNull Class<T> recordClass,
      @NonNull JsonGenerator jacksonGenerator) {
    return new JacksonWriteConsumer<>(jacksonGenerator, new ThriftWriter<>(recordClass));
  }
}
