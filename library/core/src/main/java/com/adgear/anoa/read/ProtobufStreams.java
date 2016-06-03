package com.adgear.anoa.read;

import com.google.protobuf.Message;
import com.google.protobuf.MessageLite;

import com.adgear.anoa.Anoa;
import com.adgear.anoa.AnoaHandler;
import com.fasterxml.jackson.core.JsonParser;

import java.io.InputStream;
import java.util.stream.Stream;

/**
 * Utility class for deserializing Protobuf records in a {@link java.util.stream.Stream}.
 */
final public class ProtobufStreams {

  private ProtobufStreams() {
  }

  /**
   * Stream from Protobuf delimited binary representations, i.e. as written by {@code
   * MessageLite#writeDelimitedTo(OutputStream)}.
   *
   * @param recordClass Protobuf record class object
   * @param inputStream stream from which to deserialize
   * @param <R>         Protobuf record type
   */
  static public <R extends MessageLite> Stream<R> binary(
      Class<R> recordClass,
      InputStream inputStream) {
    return LookAheadIteratorFactory.protobuf(inputStream, recordClass, false)
        .asStream();
  }


  /**
   * Stream from Protobuf delimited binary representations, i.e. as written by {@code
   * MessageLite#writeDelimitedTo(OutputStream)}. An exception is raised whenever a required field
   * is missing
   *
   * @param recordClass Protobuf record class object
   * @param inputStream stream from which to deserialize
   * @param <R>         Protobuf record type
   */
  @Deprecated
  static public <R extends MessageLite> Stream<R> binaryStrict(
      Class<R> recordClass,
      InputStream inputStream) {
    return LookAheadIteratorFactory.protobuf(inputStream, recordClass, true)
        .asStream();
  }

  /**
   * Stream from Protobuf delimited binary representations, i.e. as written by {@code
   * MessageLite#writeDelimitedTo(OutputStream)}.
   *
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param recordClass Protobuf record class object
   * @param inputStream stream from which to deserialize
   * @param <R>         Protobuf record type
   * @param <M>         Metadata type
   */
  static public <R extends MessageLite, M> Stream<Anoa<R, M>> binary(
      AnoaHandler<M> anoaHandler,
      Class<R> recordClass,
      InputStream inputStream) {
    return LookAheadIteratorFactory.protobuf(anoaHandler, inputStream, recordClass, false)
        .asStream();
  }

  /**
   * Stream from Protobuf delimited binary representations, i.e. as written by {@code
   * MessageLite#writeDelimitedTo(OutputStream)}.
   *
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param recordClass Protobuf record class object
   * @param inputStream stream from which to deserialize
   * @param <R>         Protobuf record type
   * @param <M>         Metadata type
   */
  @Deprecated
  static public <R extends MessageLite, M> Stream<Anoa<R, M>> binaryStrict(
      AnoaHandler<M> anoaHandler,
      Class<R> recordClass,
      InputStream inputStream) {
    return LookAheadIteratorFactory.protobuf(anoaHandler, inputStream, recordClass, true)
        .asStream();
  }

  /**
   * Stream with 'natural' object-mapping from JsonParser instance.
   *
   * @param recordClass   Protobuf record class object
   * @param jacksonParser JsonParser instance from which to read
   * @param <R>           Protobuf record type
   */
  static public <R extends Message> Stream<R> jackson(
      Class<R> recordClass,
      JsonParser jacksonParser) {
    return new ProtobufReader<>(recordClass).stream(jacksonParser);
  }


  /**
   * Stream with 'natural' object-mapping from JsonParser instance, with strict type checking.
   *
   * @param recordClass   Protobuf record class object
   * @param jacksonParser JsonParser instance from which to read
   * @param <R>           Protobuf record type
   */
  static public <R extends Message> Stream<R> jacksonStrict(
      Class<R> recordClass,
      JsonParser jacksonParser) {
    return new ProtobufReader<>(recordClass).streamStrict(jacksonParser);
  }

  /**
   * Stream with 'natural' object-mapping from JsonParser instance.
   *
   * @param anoaHandler   {@code AnoaHandler} instance to use for exception handling
   * @param recordClass   Protobuf record class object
   * @param jacksonParser JsonParser instance from which to read
   * @param <R>           Protobuf record type
   * @param <M>           Metadata type
   */
  static public <R extends Message, M> Stream<Anoa<R, M>> jackson(
      AnoaHandler<M> anoaHandler,
      Class<R> recordClass,
      JsonParser jacksonParser) {
    return new ProtobufReader<>(recordClass).stream(anoaHandler, jacksonParser);
  }

  /**
   * Stream with 'natural' object-mapping from JsonParser instance, with strict type checking.
   *
   * @param anoaHandler   {@code AnoaHandler} instance to use for exception handling
   * @param recordClass   Protobuf record class object
   * @param jacksonParser JsonParser instance from which to read
   * @param <R>           Protobuf record type
   * @param <M>           Metadata type
   */
  static public <R extends Message, M> Stream<Anoa<R, M>> jacksonStrict(
      AnoaHandler<M> anoaHandler,
      Class<R> recordClass,
      JsonParser jacksonParser) {
    return new ProtobufReader<>(recordClass).streamStrict(anoaHandler, jacksonParser);
  }
}
