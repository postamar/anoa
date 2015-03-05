package com.adgear.anoa.factory;

import checkers.nullness.quals.NonNull;

import com.adgear.anoa.factory.util.ThriftReadIterator;

import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFileTransport;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class ThriftStreams {

  static public <T extends TBase<T, ? extends TFieldIdEnum>> @NonNull Stream<T> compact(
      @NonNull Supplier<T> supplier,
      @NonNull InputStream inputStream) {
    return compact(supplier, new TIOStreamTransport(inputStream));
  }

  static public <T extends TBase<T, ? extends TFieldIdEnum>> @NonNull Stream<T> compact(
      @NonNull Supplier<T> supplier,
      @NonNull String fileName,
      boolean readOnly) {
    try {
      return compact(supplier, new TFileTransport(fileName, readOnly));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public <T extends TBase<T, ? extends TFieldIdEnum>> @NonNull Stream<T> compact(
      @NonNull Supplier<T> supplier,
      @NonNull TTransport tTransport) {
    return from(supplier, tTransport, TCompactProtocol::new);
  }

  static public <T extends TBase<T, ? extends TFieldIdEnum>> @NonNull Stream<T> binary(
      @NonNull Supplier<T> supplier,
      @NonNull InputStream inputStream) {
    return binary(supplier, new TIOStreamTransport(new BufferedInputStream(inputStream)));
  }

  static public <T extends TBase<T, ? extends TFieldIdEnum>> @NonNull Stream<T> binary(
      @NonNull Supplier<T> supplier,
      @NonNull String fileName,
      boolean readOnly) {
    try {
      return binary(supplier, new TFileTransport(fileName, readOnly));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public <T extends TBase<T, ? extends TFieldIdEnum>> @NonNull Stream<T> binary(
      @NonNull Supplier<T> supplier,
      @NonNull TTransport tTransport) {
    return from(supplier, tTransport, TBinaryProtocol::new);
  }

  static public <T extends TBase<T, ? extends TFieldIdEnum>> @NonNull Stream<T> json(
      @NonNull Supplier<T> supplier,
      @NonNull InputStream inputStream) {
    return json(supplier, new TIOStreamTransport(new BufferedInputStream(inputStream)));
  }

  static public <T extends TBase<T, ? extends TFieldIdEnum>> @NonNull Stream<T> json(
      @NonNull Supplier<T> supplier,
      @NonNull String fileName,
      boolean readOnly) {
    try {
      return json(supplier, new TFileTransport(fileName, readOnly));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public <T extends TBase<T, ? extends TFieldIdEnum>> @NonNull Stream<T> json(
      @NonNull Supplier<T> supplier,
      @NonNull TTransport tTransport) {
    return from(supplier, tTransport, TJSONProtocol::new);
  }

  static protected <T extends TBase<T, ? extends TFieldIdEnum>> Stream<T> from(
      Supplier<T> supplier,
      TTransport tTransport,
      Function<TTransport,TProtocol> cFn) {
    return new ThriftReadIterator<>(tTransport, cFn.apply(tTransport), supplier).stream();
  }

}
