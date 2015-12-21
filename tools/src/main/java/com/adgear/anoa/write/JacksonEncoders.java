package com.adgear.anoa.write;

import com.adgear.anoa.Anoa;
import com.adgear.anoa.AnoaHandler;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import java.util.function.Function;

/**
 * Utility class for generating functions for serializing Jackson ObjectNode records. Unless
 * specified otherwise, the functions should not be deemed thread-safe.
 */
public class JacksonEncoders {

  protected JacksonEncoders() {
  }

  /**
   * @return A function which serializes an ObjectNode into its JSON encoding
   */
  static public Function<ObjectNode, byte[]> json() {
    return toBytes(new JsonConsumers()::to);
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param <M>         Metadata type
   * @return A function which serializes an ObjectNode record into its JSON encoding
   */
  static public <M> Function<Anoa<ObjectNode, M>, Anoa<byte[], M>> json(
      AnoaHandler<M> anoaHandler) {
    return toBytes(anoaHandler, new JsonConsumers()::to);
  }

  /**
   * @return A function which serializes an ObjectNode into its CBOR encoding
   */
  static public Function<ObjectNode, byte[]> cbor() {
    return toBytes(new CborConsumers()::to);
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param <M>         Metadata type
   * @return A function which serializes an ObjectNode record into its CBOR encoding
   */
  static public <M> Function<Anoa<ObjectNode, M>, Anoa<byte[], M>> cbor(
      AnoaHandler<M> anoaHandler) {
    return toBytes(anoaHandler, new CborConsumers()::to);
  }

  /**
   * @param csvSchema CSV schema specification (separator, etc.)
   * @return A function which serializes an ObjectNode into a CSV encoding
   */
  static public Function<ObjectNode, String> csv(CsvSchema csvSchema) {
    return toBytes(new CsvConsumers(csvSchema)::to).andThen(String::new);
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param csvSchema   CSV schema specification (separator, etc.)
   * @param <M>         Metadata type
   * @return A function which serializes an ObjectNode record into a CSV encoding
   */
  static public <M> Function<Anoa<ObjectNode, M>, Anoa<byte[], M>> csv(
      AnoaHandler<M> anoaHandler,
      CsvSchema csvSchema) {
    return toBytes(anoaHandler, new CsvConsumers(csvSchema)::to);
  }

  static protected Function<ObjectNode, byte[]> toBytes(
      Function<ByteArrayOutputStream, WriteConsumer<ObjectNode>> fn) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    WriteConsumer<ObjectNode> wc = fn.apply(baos);
    return node -> {
      baos.reset();
      wc.accept(node);
      wc.flushUnchecked();
      return baos.toByteArray();
    };
  }

  static protected <M> Function<Anoa<ObjectNode, M>, Anoa<byte[], M>> toBytes(
      AnoaHandler<M> anoaHandler,
      Function<ByteArrayOutputStream, WriteConsumer<ObjectNode>> fn) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    WriteConsumer<ObjectNode> wc = fn.apply(baos);
    return anoaHandler.functionChecked(node -> {
      baos.reset();
      wc.acceptChecked(node);
      wc.flush();
      return baos.toByteArray();
    });
  }
}
