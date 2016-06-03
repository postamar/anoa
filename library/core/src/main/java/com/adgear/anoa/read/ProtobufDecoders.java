package com.adgear.anoa.read;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;

import com.adgear.anoa.Anoa;
import com.adgear.anoa.AnoaHandler;
import com.adgear.anoa.AnoaReflectionUtils;
import com.fasterxml.jackson.core.JsonParser;

import java.io.UncheckedIOException;
import java.util.function.Function;

/**
 * Utility class for generating functions for deserializing Protobuf records. Unless specified
 * otherwise, the functions should not be deemed thread-safe.
 */
final public class ProtobufDecoders {

  private ProtobufDecoders() {
  }

  /**
   * @param recordClass Protobuf record class object
   * @param <R>         Protobuf record type
   * @return A thread-safe function which deserializes a Protobuf record from its binary encoding.
   */
  static public <R extends MessageLite> Function<byte[], R> binary(
      Class<R> recordClass) {
    Parser<R> parser = AnoaReflectionUtils.getProtobufParser(recordClass);
    return (byte[] bytes) -> {
      try {
        return parser.parsePartialFrom(bytes);
      } catch (InvalidProtocolBufferException e) {
        throw new UncheckedIOException(e);
      }
    };
  }

  /**
   * @param recordClass Protobuf record class object
   * @param <R>         Protobuf record type
   * @return A thread-safe function which deserializes a Protobuf record from its binary encoding.
   * The lambda will raise an exception whenever a required field is missing.
   */
  @Deprecated
  static public <R extends MessageLite> Function<byte[], R> binaryStrict(
      Class<R> recordClass) {
    Parser<R> parser = AnoaReflectionUtils.getProtobufParser(recordClass);
    return (byte[] bytes) -> {
      try {
        return parser.parseFrom(bytes);
      } catch (InvalidProtocolBufferException e) {
        throw new UncheckedIOException(e);
      }
    };
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param recordClass Protobuf record class object
   * @param <R>         Protobuf record type
   * @param <M>         Metadata type
   * @return A function which deserializes a Protobuf record from its binary encoding.
   */
  static public <R extends MessageLite, M> Function<Anoa<byte[], M>, Anoa<R, M>> binary(
      AnoaHandler<M> anoaHandler,
      Class<R> recordClass) {
    Parser<R> parser = AnoaReflectionUtils.getProtobufParser(recordClass);
    return anoaHandler.functionChecked(parser::parsePartialFrom);
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param recordClass Protobuf record class object
   * @param <R>         Protobuf record type
   * @param <M>         Metadata type
   * @return A function which deserializes a Protobuf record from its binary encoding.
   */
  @Deprecated
  static public <R extends MessageLite, M> Function<Anoa<byte[], M>, Anoa<R, M>> binaryStrict(
      AnoaHandler<M> anoaHandler,
      Class<R> recordClass) {
    Parser<R> parser = AnoaReflectionUtils.getProtobufParser(recordClass);
    return anoaHandler.functionChecked(parser::parseFrom);
  }

  /**
   * @param recordClass Protobuf record class object
   * @param <P>         Jackson JsonParser type
   * @param <R>         Protobuf record type
   * @return A function which reads a Protobuf record from a JsonParser, in its 'natural' encoding.
   */
  static public <P extends JsonParser, R extends Message> Function<P, R> jackson(
      Class<R> recordClass) {
    return new ProtobufReader<>(recordClass).decoder();
  }

  /**
   * @param recordClass Protobuf record class object
   * @param <P>         Jackson JsonParser type
   * @param <R>         Protobuf record type
   * @return A function which reads a Protobuf record from a JsonParser, in its 'natural' encoding,
   * with strictest possible type checking.
   */
  static public <P extends JsonParser, R extends Message> Function<P, R> jacksonStrict(
      Class<R> recordClass) {
    return new ProtobufReader<>(recordClass).decoderStrict();
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param recordClass Protobuf record class object
   * @param <P>         Jackson JsonParser type
   * @param <R>         Protobuf record type
   * @param <M>         Metadata type
   * @return A function which reads a Protobuf record from a JsonParser, in its 'natural' encoding.
   */
  static public <P extends JsonParser, R extends Message, M>
  Function<Anoa<P, M>, Anoa<R, M>> jackson(
      AnoaHandler<M> anoaHandler,
      Class<R> recordClass) {
    return new ProtobufReader<>(recordClass).decoder(anoaHandler);
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param recordClass Protobuf record class object
   * @param <P>         Jackson JsonParser type
   * @param <R>         Protobuf record type
   * @param <M>         Metadata type
   * @return A function which reads a Protobuf record from a JsonParser, in its 'natural' encoding,
   * with strictest possible type checking.
   */
  static public <P extends JsonParser, R extends Message, M>
  Function<Anoa<P, M>, Anoa<R, M>> jacksonStrict(
      AnoaHandler<M> anoaHandler,
      Class<R> recordClass) {
    return new ProtobufReader<>(recordClass).decoderStrict(anoaHandler);
  }
}
