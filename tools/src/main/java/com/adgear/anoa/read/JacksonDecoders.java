package com.adgear.anoa.read;

import com.adgear.anoa.Anoa;
import com.adgear.anoa.AnoaHandler;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import java.util.function.Function;

/**
 * Utility class for generating functions for deserializing Jackson ObjectNode records.
 * Unless specified otherwise, the functions should not be deemed thread-safe.
 */
public class JacksonDecoders {

  /**
   * @return A function which deserializes an ObjectNode from its JSON encoding
   */
  static public Function<byte[], ObjectNode> json() {
    JsonStreams streams = new JsonStreams();
    return bytes -> streams.decode(streams.parser(bytes));
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param <M> Metadata type
   * @return A function which deserializes an ObjectNode record from its JSON encoding
   */
  static public <M> Function<Anoa<byte[], M>, Anoa<ObjectNode, M>> json(
      AnoaHandler<M> anoaHandler) {
    JsonStreams streams = new JsonStreams();
    return anoaHandler.functionChecked(
        bytes -> streams.decodeChecked(streams.parserChecked(bytes)));
  }

  /**
   * @return A function which deserializes an ObjectNode from its CBOR encoding
   */
  static public Function<byte[], ObjectNode> cbor() {
    CborStreams streams = new CborStreams();
    return bytes -> streams.decode(streams.parser(bytes));
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param <M> Metadata type
   * @return A function which deserializes an ObjectNode record from its CBOR encoding
   */
  static public <M> Function<Anoa<byte[], M>, Anoa<ObjectNode, M>> cbor(
      AnoaHandler<M> anoaHandler) {
    CborStreams streams = new CborStreams();
    return anoaHandler.functionChecked(
        bytes -> streams.decodeChecked(streams.parserChecked(bytes)));
  }

  /**
   * @param csvSchema CSV schema specification (separator, etc.)
   * @return A function which deserializes an ObjectNode from a CSV encoding
   */
  static public Function<String, ObjectNode> csv(CsvSchema csvSchema) {
    CsvStreams streams = new CsvStreams(csvSchema);
    return string -> streams.decode(streams.parser(string));
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param csvSchema CSV schema specification (separator, etc.)
   * @param <M> Metadata type
   * @return A function which deserializes an ObjectNode record from a CSV encoding
   */
  static public <M> Function<Anoa<String, M>, Anoa<ObjectNode, M>> csv(
      AnoaHandler<M> anoaHandler,
      CsvSchema csvSchema) {
    CsvStreams streams = new CsvStreams(csvSchema);
    return anoaHandler.functionChecked(
        string -> streams.decodeChecked(streams.parserChecked(string)));
  }
}
