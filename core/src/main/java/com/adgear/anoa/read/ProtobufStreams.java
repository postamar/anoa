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
public class ProtobufStreams {

  protected ProtobufStreams() {
  }

  /**
   * Stream from Protobuf delimited binary representations, i.e. as written by {@code
   * MessageLite#writeDelimitedTo(OutputStream)}.
   *
   * @param recordClass Protobuf record class object
   * @param strict      when true, an exception is raised whenever a required field is missing
   * @param inputStream stream from which to deserialize
   * @param <R>         Protobuf record type
   */
  static public <R extends MessageLite> Stream<R> binary(
      Class<R> recordClass,
      boolean strict,
      InputStream inputStream) {
    return LookAheadIteratorFactory.protobuf(inputStream, recordClass, strict)
        .asStream();
  }

  /**
   * Stream from Protobuf delimited binary representations, i.e. as written by {@code
   * MessageLite#writeDelimitedTo(OutputStream)}.
   *
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param recordClass Protobuf record class object
   * @param strict      when true, an exception is raised whenever a required field is missing
   * @param inputStream stream from which to deserialize
   * @param <R>         Protobuf record type
   * @param <M>         Metadata type
   */
  static public <R extends MessageLite, M> Stream<Anoa<R, M>> binary(
      AnoaHandler<M> anoaHandler,
      Class<R> recordClass,
      boolean strict,
      InputStream inputStream) {
    return LookAheadIteratorFactory.protobuf(anoaHandler, inputStream, recordClass, strict)
        .asStream();
  }

  /**
   * Stream with 'natural' object-mapping from JsonParser instance
   *
   * @param recordClass   Protobuf record class object
   * @param strict        enable strict type checking
   * @param jacksonParser JsonParser instance from which to read
   * @param <R>           Protobuf record type
   */
  static public <R extends Message> Stream<R> jackson(
      Class<R> recordClass,
      boolean strict,
      JsonParser jacksonParser) {
    return JacksonUtils.stream(new ProtobufReader<>(recordClass), strict, jacksonParser);
  }

  /**
   * Stream with 'natural' object-mapping from JsonParser instance
   *
   * @param anoaHandler   {@code AnoaHandler} instance to use for exception handling
   * @param recordClass   Protobuf record class object
   * @param strict        enable strict type checking
   * @param jacksonParser JsonParser instance from which to read
   * @param <R>           Protobuf record type
   * @param <M>           Metadata type
   */
  static public <R extends Message, M> Stream<Anoa<R, M>> jackson(
      AnoaHandler<M> anoaHandler,
      Class<R> recordClass,
      boolean strict,
      JsonParser jacksonParser) {
    return JacksonUtils.stream(anoaHandler,
                               new ProtobufReader<>(recordClass),
                               strict,
                               jacksonParser);
  }
}
