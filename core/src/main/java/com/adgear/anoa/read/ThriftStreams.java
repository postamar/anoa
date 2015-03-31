package com.adgear.anoa.read;

import com.adgear.anoa.Anoa;
import com.adgear.anoa.AnoaHandler;
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

/**
 * Utility class for deserializing Thrift records in a {@link java.util.stream.Stream}.
 */
public class ThriftStreams {

  protected ThriftStreams() {
  }

  /**
   * Stream from Thrift compact binary representations
   *
   * @param supplier Thrift record instance supplier
   * @param inputStream stream from which to deserialize
   * @param <T> Thrift record type
   */
  static public <T extends TBase> /*@NonNull*/ Stream<T> compact(
      /*@NonNull*/ Supplier<T> supplier,
      /*@NonNull*/ InputStream inputStream) {
    return compact(supplier, new TIOStreamTransport(inputStream));
  }

  /**
   * Stream from Thrift compact binary representations
   *
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param supplier Thrift record instance supplier
   * @param inputStream stream from which to deserialize
   * @param <T> Thrift record type
   * @param <M> Metadata type
   */
  static public <T extends TBase, M> /*@NonNull*/ Stream<Anoa<T, M>> compact(
      /*@NonNull*/ AnoaHandler<M> anoaHandler,
      /*@NonNull*/ Supplier<T> supplier,
      /*@NonNull*/ InputStream inputStream) {
    return compact(anoaHandler, supplier, new TIOStreamTransport(inputStream));
  }

  /**
   * Stream from Thrift compact binary representations
   *
   * @param supplier Thrift record instance supplier
   * @param fileName name of file from which to read
   * @param readOnly file open mode
   * @param <T> Thrift record type
   */
  static public <T extends TBase> /*@NonNull*/ Stream<T> compact(
      /*@NonNull*/ Supplier<T> supplier,
      /*@NonNull*/ String fileName,
      boolean readOnly) {
    try {
      return compact(supplier, new TFileTransport(fileName, readOnly));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Stream from Thrift compact binary representations
   *
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param supplier Thrift record instance supplier
   * @param fileName name of file from which to read
   * @param readOnly file open mode
   * @param <T> Thrift record type
   * @param <M> Metadata type
   */
  static public <T extends TBase, M> /*@NonNull*/ Stream<Anoa<T, M>> compact(
      /*@NonNull*/ AnoaHandler<M> anoaHandler,
      /*@NonNull*/ Supplier<T> supplier,
      /*@NonNull*/ String fileName,
      boolean readOnly) {
    try {
      return compact(anoaHandler, supplier, new TFileTransport(fileName, readOnly));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Stream from Thrift compact binary representations
   *
   * @param supplier Thrift record instance supplier
   * @param tTransport Thrift TTransport instance from which to read
   * @param <T> Thrift record type
   */
  static public <T extends TBase> /*@NonNull*/ Stream<T> compact(
      /*@NonNull*/ Supplier<T> supplier,
      /*@NonNull*/ TTransport tTransport) {
    return LookAheadIteratorFactory.thrift(new TCompactProtocol(tTransport), supplier)
        .asStream();
  }

  /**
   * Stream from Thrift compact binary representations
   *
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param supplier Thrift record instance supplier
   * @param tTransport Thrift TTransport instance from which to read
   * @param <T> Thrift record type
   * @param <M> Metadata type
   */
  static public <T extends TBase, M> /*@NonNull*/ Stream<Anoa<T, M>> compact(
      /*@NonNull*/ AnoaHandler<M> anoaHandler,
      /*@NonNull*/ Supplier<T> supplier,
      /*@NonNull*/ TTransport tTransport) {
    return LookAheadIteratorFactory.thrift(anoaHandler, new TCompactProtocol(tTransport), supplier)
        .asStream();
  }

  /**
   * Stream from Thrift standard binary representations
   *
   * @param supplier Thrift record instance supplier
   * @param inputStream stream from which to deserialize
   * @param <T> Thrift record type
   */
  static public <T extends TBase> /*@NonNull*/ Stream<T> binary(
      /*@NonNull*/ Supplier<T> supplier,
      /*@NonNull*/ InputStream inputStream) {
    return binary(supplier, new TIOStreamTransport(new BufferedInputStream(inputStream)));
  }

  /**
   * Stream from Thrift standard binary representations
   *
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param supplier Thrift record instance supplier
   * @param inputStream stream from which to deserialize
   * @param <T> Thrift record type
   * @param <M> Metadata type
   */
  static public <T extends TBase, M> /*@NonNull*/ Stream<Anoa<T, M>> binary(
      /*@NonNull*/ AnoaHandler<M> anoaHandler,
      /*@NonNull*/ Supplier<T> supplier,
      /*@NonNull*/ InputStream inputStream) {
    return binary(anoaHandler,
                  supplier,
                  new TIOStreamTransport(new BufferedInputStream(inputStream)));
  }

  /**
   * Stream from Thrift standard binary representations
   *
   * @param supplier Thrift record instance supplier
   * @param fileName name of file from which to read
   * @param readOnly file open mode
   * @param <T> Thrift record type
   */
  static public <T extends TBase> /*@NonNull*/ Stream<T> binary(
      /*@NonNull*/ Supplier<T> supplier,
      /*@NonNull*/ String fileName,
      boolean readOnly) {
    try {
      return binary(supplier, new TFileTransport(fileName, readOnly));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Stream from Thrift standard binary representations
   *
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param supplier Thrift record instance supplier
   * @param fileName name of file from which to read
   * @param readOnly file open mode
   * @param <T> Thrift record type
   * @param <M> Metadata type
   */
  static public <T extends TBase, M> /*@NonNull*/ Stream<Anoa<T, M>> binary(
      /*@NonNull*/ AnoaHandler<M> anoaHandler,
      /*@NonNull*/ Supplier<T> supplier,
      /*@NonNull*/ String fileName,
      boolean readOnly) {
    try {
      return binary(anoaHandler, supplier, new TFileTransport(fileName, readOnly));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Stream from Thrift standard binary representations
   *
   * @param supplier Thrift record instance supplier
   * @param tTransport Thrift TTransport instance from which to read
   * @param <T> Thrift record type
   */
  static public <T extends TBase> /*@NonNull*/ Stream<T> binary(
      /*@NonNull*/ Supplier<T> supplier,
      /*@NonNull*/ TTransport tTransport) {
    return LookAheadIteratorFactory.thrift(new TBinaryProtocol(tTransport), supplier).asStream();
  }

  /**
   * Stream from Thrift standard binary representations
   *
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param supplier Thrift record instance supplier
   * @param tTransport Thrift TTransport instance from which to read
   * @param <T> Thrift record type
   * @param <M> Metadata type
   */
  static public <T extends TBase, M> /*@NonNull*/ Stream<Anoa<T, M>> binary(
      /*@NonNull*/ AnoaHandler<M> anoaHandler,
      /*@NonNull*/ Supplier<T> supplier,
      /*@NonNull*/ TTransport tTransport) {
    return LookAheadIteratorFactory.thrift(anoaHandler, new TBinaryProtocol(tTransport), supplier)
        .asStream();
  }

  /**
   * Stream from Thrift JSON representations
   *
   * @param supplier Thrift record instance supplier
   * @param inputStream stream from which to deserialize
   * @param <T> Thrift record type
   */
  static public <T extends TBase> /*@NonNull*/ Stream<T> json(
      /*@NonNull*/ Supplier<T> supplier,
      /*@NonNull*/ InputStream inputStream) {
    return json(supplier, new TIOStreamTransport(new BufferedInputStream(inputStream)));
  }

  /**
   * Stream from Thrift JSON representations
   *
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param supplier Thrift record instance supplier
   * @param inputStream stream from which to deserialize
   * @param <T> Thrift record type
   * @param <M> Metadata type
   */
  static public <T extends TBase, M> /*@NonNull*/ Stream<Anoa<T, M>> json(
      /*@NonNull*/ AnoaHandler<M> anoaHandler,
      /*@NonNull*/ Supplier<T> supplier,
      /*@NonNull*/ InputStream inputStream) {
    return json(anoaHandler,
                supplier,
                new TIOStreamTransport(new BufferedInputStream(inputStream)));
  }

  /**
   * Stream from Thrift JSON representations
   *
   * @param supplier Thrift record instance supplier
   * @param fileName name of file from which to read
   * @param readOnly file open mode
   * @param <T> Thrift record type
   */
  static public <T extends TBase> /*@NonNull*/ Stream<T> json(
      /*@NonNull*/ Supplier<T> supplier,
      /*@NonNull*/ String fileName,
      boolean readOnly) {
    try {
      return json(supplier, new TFileTransport(fileName, readOnly));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Stream from Thrift JSON representations
   *
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param supplier Thrift record instance supplier
   * @param fileName name of file from which to read
   * @param readOnly file open mode
   * @param <T> Thrift record type
   * @param <M> Metadata type
   */
  static public <T extends TBase, M> /*@NonNull*/ Stream<Anoa<T, M>> json(
      /*@NonNull*/ AnoaHandler<M> anoaHandler,
      /*@NonNull*/ Supplier<T> supplier,
      /*@NonNull*/ String fileName,
      boolean readOnly) {
    try {
      return json(anoaHandler, supplier, new TFileTransport(fileName, readOnly));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Stream from Thrift JSON representations
   *
   * @param supplier Thrift record instance supplier
   * @param tTransport Thrift TTransport instance from which to read
   * @param <T> Thrift record type
   */
  static public <T extends TBase> /*@NonNull*/ Stream<T> json(
      /*@NonNull*/ Supplier<T> supplier,
      /*@NonNull*/ TTransport tTransport) {
    return LookAheadIteratorFactory.thrift(new TJSONProtocol(tTransport), supplier).asStream();
  }

  /**
   * Stream from Thrift JSON representations
   *
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param supplier Thrift record instance supplier
   * @param tTransport Thrift TTransport instance from which to read
   * @param <T> Thrift record type
   * @param <M> Metadata type
   */
  static public <T extends TBase, M> /*@NonNull*/ Stream<Anoa<T, M>> json(
      /*@NonNull*/ AnoaHandler<M> anoaHandler,
      /*@NonNull*/ Supplier<T> supplier,
      /*@NonNull*/ TTransport tTransport) {
    return LookAheadIteratorFactory.thrift(anoaHandler, new TJSONProtocol(tTransport), supplier).asStream();
  }

  /**
   * Stream with 'natural' object-mapping from JsonParser instance
   *
   * @param recordClass Thrift record class object
   * @param strict enable strict type checking
   * @param jacksonParser JsonParser instance from which to read
   * @param <T> Thrift record type
   */
  static public <T extends TBase> /*@NonNull*/ Stream<T> jackson(
      /*@NonNull*/ Class<T> recordClass,
      boolean strict,
      /*@NonNull*/ JsonParser jacksonParser) {
    return LookAheadIteratorFactory.jackson(jacksonParser).asStream()
        .map(TreeNode::traverse)
        .map(ThriftDecoders.jackson(recordClass, strict));
  }

  /**
   * Stream with 'natural' object-mapping from JsonParser instance
   *
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param recordClass Thrift record class object
   * @param strict enable strict type checking
   * @param jacksonParser JsonParser instance from which to read
   * @param <T> Thrift record type
   * @param <M> Metadata type
   */
  static public <T extends TBase, M>/*@NonNull*/ Stream<Anoa<T, M>> jackson(
      /*@NonNull*/ AnoaHandler<M> anoaHandler,
      /*@NonNull*/ Class<T> recordClass,
      boolean strict,
      /*@NonNull*/ JsonParser jacksonParser) {
    return LookAheadIteratorFactory.jackson(anoaHandler, jacksonParser).asStream()
        .map(anoaHandler.function(TreeNode::traverse))
        .map(ThriftDecoders.jackson(anoaHandler, recordClass, strict));
  }
}
