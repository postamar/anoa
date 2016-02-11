package com.adgear.anoa.write;

import com.google.protobuf.Message;
import com.google.protobuf.MessageLite;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.OutputStream;

/**
 * Utility class for generating {@code WriteConsumer} instances to write Protobuf records.
 */
public class ProtobufConsumers {

  protected ProtobufConsumers() {
  }

  /**
   * Write as delimited binary blobs, as per {@code MessageLite#writeDelimitedTo(OutputStream)}
   *
   * @param outputStream stream to write into
   * @param <R>          Protobuf record type
   */
  static public <R extends MessageLite> WriteConsumer<R> binary(
      OutputStream outputStream) {
    return new ProtobufWriteConsumer<>(outputStream);
  }

  /**
   * Write as 'natural' JSON serializations using provided generator
   *
   * @param recordClass      Protobuf record class object
   * @param jacksonGenerator JsonGenerator instance to write into
   * @param <R>              Protobuf record type
   */
  static public <R extends Message> WriteConsumer<R> jackson(
      Class<R> recordClass,
      JsonGenerator jacksonGenerator) {
    return new JacksonWriteConsumer<>(jacksonGenerator, new ProtobufWriter<>(recordClass));
  }
}
