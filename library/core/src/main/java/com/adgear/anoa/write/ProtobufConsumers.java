package com.adgear.anoa.write;

import com.google.protobuf.Message;
import com.google.protobuf.MessageLite;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.OutputStream;

/**
 * Utility class for generating {@code WriteConsumer} instances to write Protobuf records.
 */
final public class ProtobufConsumers {

  private ProtobufConsumers() {
  }

  /**
   * Write as delimited binary blobs, as per {@code MessageLite#writeDelimitedTo(OutputStream)}.
   *
   * @param outputStream stream to write into
   * @param <R>          Protobuf record type
   */
  static public <R extends MessageLite> WriteConsumer<R> binary(
      OutputStream outputStream) {
    return new ProtobufWriteConsumer<>(outputStream);
  }

  /**
   * Write as 'natural' JSON serializations using provided generator, in compact form.
   *
   * @param recordClass      Protobuf record class object
   * @param jacksonGenerator JsonGenerator instance to write into
   * @param <R>              Protobuf record type
   */
  static public <R extends Message> WriteConsumer<R> jackson(
      Class<R> recordClass,
      JsonGenerator jacksonGenerator) {
    return new ProtobufWriter<>(recordClass).writeConsumer(jacksonGenerator);
  }


  /**
   * Write as 'natural' JSON serializations using provided generator, in strict form.
   *
   * @param recordClass      Protobuf record class object
   * @param jacksonGenerator JsonGenerator instance to write into
   * @param <R>              Protobuf record type
   */
  static public <R extends Message> WriteConsumer<R> jacksonStrict(
      Class<R> recordClass,
      JsonGenerator jacksonGenerator) {
    return new ProtobufWriter<>(recordClass).writeConsumerStrict(jacksonGenerator);
  }
}
