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
public class ProtobufDecoders {

  protected ProtobufDecoders() {
  }

  /**
   * @param recordClass Protobuf record class object
   * @param strict      when true, an exception is raised whenever a required field is missing
   * @param <R>         Protobuf record type
   * @return A thread-safe function which deserializes a Protobuf record from its binary encoding.
   */
  static public <R extends MessageLite> Function<byte[], R> binary(
      Class<R> recordClass,
      boolean strict) {
    Parser<R> parser = AnoaReflectionUtils.getProtobufParser(recordClass);
    if (strict) {
      return (byte[] bytes) -> {
        try {
          return parser.parseFrom(bytes);
        } catch (InvalidProtocolBufferException e) {
          throw new UncheckedIOException(e);
        }
      };
    } else {
      return (byte[] bytes) -> {
        try {
          return parser.parsePartialFrom(bytes);
        } catch (InvalidProtocolBufferException e) {
          throw new UncheckedIOException(e);
        }
      };
    }
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param recordClass Protobuf record class object
   * @param strict      when true, an exception is raised whenever a required field is missing
   * @param <R>         Protobuf record type
   * @param <M>         Metadata type
   * @return A function  which deserializes a Protobuf record from its binary encoding.
   */
  static public <R extends MessageLite, M> Function<Anoa<byte[], M>, Anoa<R, M>> binary(
      AnoaHandler<M> anoaHandler,
      Class<R> recordClass,
      boolean strict) {
    Parser<R> parser = AnoaReflectionUtils.getProtobufParser(recordClass);
    if (strict) {
      return anoaHandler.functionChecked((byte[] bytes) -> parser.parseFrom(bytes));
    } else {
      return anoaHandler.functionChecked((byte[] bytes) -> parser.parsePartialFrom(bytes));
    }
  }

  /**
   * @param recordClass Protobuf record class object
   * @param strict      enable strict type checking
   * @param <P>         Jackson JsonParser type
   * @param <R>         Protobuf record type
   * @return A function which reads a Protobuf record from a JsonParser, in its 'natural' encoding.
   */
  static public <P extends JsonParser, R extends Message> Function<P, R> jackson(
      Class<R> recordClass,
      boolean strict) {
    return new ProtobufReader<>(recordClass).decoder(strict);
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param recordClass Protobuf record class object
   * @param strict      enable strict type checking
   * @param <P>         Jackson JsonParser type
   * @param <R>         Protobuf record type
   * @param <M>         Metadata type
   * @return A function which reads a Protobuf record from a JsonParser, in its 'natural' encoding.
   */
  static public <P extends JsonParser, R extends Message, M>
  Function<Anoa<P, M>, Anoa<R, M>> jackson(
      AnoaHandler<M> anoaHandler,
      Class<R> recordClass,
      boolean strict) {
    return new ProtobufReader<>(recordClass).decoder(anoaHandler, strict);
  }
}
