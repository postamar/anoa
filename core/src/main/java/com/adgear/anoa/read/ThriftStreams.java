package com.adgear.anoa.read;

import checkers.nullness.quals.NonNull;

import com.adgear.anoa.Anoa;
import com.adgear.anoa.AnoaFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.TreeNode;

import org.apache.thrift.TBase;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.transport.TFileTransport;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class ThriftStreams {

  static public <T extends TBase, M> @NonNull Stream<Anoa<T, M>> compact(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull Supplier<T> supplier,
      @NonNull InputStream inputStream) {
    return compact(anoaFactory, supplier, new TIOStreamTransport(inputStream));
  }

  static public <T extends TBase> @NonNull Stream<T> compact(
      @NonNull Supplier<T> supplier,
      @NonNull InputStream inputStream) {
    return compact(supplier, new TIOStreamTransport(inputStream));
  }

  static public <T extends TBase> @NonNull Stream<T> compact(
      @NonNull Supplier<T> supplier,
      @NonNull String fileName,
      boolean readOnly) {
    try {
      return compact(supplier, new TFileTransport(fileName, readOnly));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public <T extends TBase, M> @NonNull Stream<Anoa<T, M>> compact(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull Supplier<T> supplier,
      @NonNull String fileName,
      boolean readOnly) {
    try {
      return compact(anoaFactory, supplier, new TFileTransport(fileName, readOnly));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public <T extends TBase> @NonNull Stream<T> compact(
      @NonNull Supplier<T> supplier,
      @NonNull TTransport tTransport) {
    return ReadIteratorUtils.thrift(new TCompactProtocol(tTransport), supplier)
        .stream();
  }

  static public <T extends TBase, M> @NonNull Stream<Anoa<T, M>> compact(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull Supplier<T> supplier,
      @NonNull TTransport tTransport) {
    return ReadIteratorUtils.thrift(anoaFactory, new TCompactProtocol(tTransport), supplier)
        .stream();
  }

  static public <T extends TBase, M> @NonNull Stream<Anoa<T, M>> binary(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull Supplier<T> supplier,
      @NonNull InputStream inputStream) {
    return binary(anoaFactory,
                  supplier,
                  new TIOStreamTransport(new BufferedInputStream(inputStream)));
  }

  static public <T extends TBase> @NonNull Stream<T> binary(
      @NonNull Supplier<T> supplier,
      @NonNull InputStream inputStream) {
    return binary(supplier, new TIOStreamTransport(new BufferedInputStream(inputStream)));
  }

  static public <T extends TBase> @NonNull Stream<T> binary(
      @NonNull Supplier<T> supplier,
      @NonNull String fileName,
      boolean readOnly) {
    try {
      return binary(supplier, new TFileTransport(fileName, readOnly));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public <T extends TBase, M> @NonNull Stream<Anoa<T, M>> binary(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull Supplier<T> supplier,
      @NonNull String fileName,
      boolean readOnly) {
    try {
      return binary(anoaFactory, supplier, new TFileTransport(fileName, readOnly));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public <T extends TBase> @NonNull Stream<T> binary(
      @NonNull Supplier<T> supplier,
      @NonNull TTransport tTransport) {
    return ReadIteratorUtils.thrift(new TBinaryProtocol(tTransport), supplier).stream();
  }

  static public <T extends TBase, M> @NonNull Stream<Anoa<T, M>> binary(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull Supplier<T> supplier,
      @NonNull TTransport tTransport) {
    return ReadIteratorUtils.thrift(anoaFactory, new TBinaryProtocol(tTransport), supplier)
        .stream();
  }

  static public <T extends TBase> @NonNull Stream<T> json(
      @NonNull Supplier<T> supplier,
      @NonNull InputStream inputStream) {
    return json(supplier, new TIOStreamTransport(new BufferedInputStream(inputStream)));
  }

  static public <T extends TBase, M> @NonNull Stream<Anoa<T, M>> json(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull Supplier<T> supplier,
      @NonNull InputStream inputStream) {
    return json(anoaFactory,
                supplier,
                new TIOStreamTransport(new BufferedInputStream(inputStream)));
  }

  static public <T extends TBase> @NonNull Stream<T> json(
      @NonNull Supplier<T> supplier,
      @NonNull String fileName,
      boolean readOnly) {
    try {
      return json(supplier, new TFileTransport(fileName, readOnly));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public <T extends TBase, M> @NonNull Stream<Anoa<T, M>> json(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull Supplier<T> supplier,
      @NonNull String fileName,
      boolean readOnly) {
    try {
      return json(anoaFactory, supplier, new TFileTransport(fileName, readOnly));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public <T extends TBase, M> @NonNull Stream<Anoa<T, M>> json(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull Supplier<T> supplier,
      @NonNull TTransport tTransport) {
    return ReadIteratorUtils.thrift(anoaFactory, new TJSONProtocol(tTransport), supplier).stream();
  }

  static public <T extends TBase> @NonNull Stream<T> json(
      @NonNull Supplier<T> supplier,
      @NonNull TTransport tTransport) {
    return ReadIteratorUtils.thrift(new TJSONProtocol(tTransport), supplier).stream();
  }

  static public <T extends TBase>
  @NonNull Stream<T> jackson(
      @NonNull Class<T> recordClass,
      boolean strict,
      @NonNull JsonParser jacksonParser) {
    return ReadIteratorUtils.jackson(jacksonParser).stream()
        .map(TreeNode::traverse)
        .map(ThriftDecoders.jackson(recordClass, strict));
  }

  static public <T extends TBase, M>
  @NonNull Stream<Anoa<T, M>> jackson(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull Class<T> recordClass,
      boolean strict,
      @NonNull JsonParser jacksonParser) {
    return ReadIteratorUtils.jackson(anoaFactory, jacksonParser).stream()
        .map(anoaFactory.function(TreeNode::traverse))
        .map(ThriftDecoders.jackson(anoaFactory, recordClass, strict));
  }
}
