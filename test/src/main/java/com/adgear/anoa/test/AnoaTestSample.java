package com.adgear.anoa.test;

import com.adgear.anoa.test.ad_exchange.AdExchangeProtobuf;
import com.adgear.anoa.test.ad_exchange.LogEventAvro;
import com.adgear.anoa.test.ad_exchange.LogEventThrift;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.avro.generic.GenericRecord;
import org.jooq.lambda.Unchecked;
import org.jooq.lambda.fi.util.function.CheckedConsumer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import junitx.framework.ListAssert;

public class AnoaTestSample extends TestSample {

  static public final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  protected final List<JsonNode> jsonObjects = json()
      .map(Unchecked.function(OBJECT_MAPPER::readTree))
      .collect(Collectors.toList());


  public JsonParser jsonNullsObjectParser() {
    String json = "{\"properties\":null,\"timestamp\":null,\"type\":null,\"uuid\":null,\"request\":null,\"response\":null}";
    return Unchecked.supplier(() -> AnoaTestSample.OBJECT_MAPPER.getFactory().createParser(json))
        .get();
  }

  public Stream<JsonNode> jsonNodes() {
    return jsonObjects.stream().sequential();
  }

  public Stream<ObjectNode> jsonObjects() {
    return jsonNodes().map(node -> (ObjectNode) node);
  }

  public InputStream allAsInputStream(CheckedConsumer<OutputStream> lambda) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Unchecked.consumer(lambda).accept(baos);
    return new ByteArrayInputStream(baos.toByteArray());
  }


  public void assertJsonNodes(Stream<JsonNode> stream) {
    ListAssert.assertEquals(jsonObjects, stream.collect(Collectors.toList()));
  }

  public void assertJsonObjects(Stream<ObjectNode> stream) {
    ListAssert.assertEquals(jsonObjects, stream.collect(Collectors.toList()));
  }

  public InputStream jsonInputStream(int readFailureIndex) {
    return new TestInputStream(json().map(String::getBytes), readFailureIndex);
  }

  public JsonParser jsonParser(int readFailureIndex) {
    try {
      return OBJECT_MAPPER.getFactory().createParser(jsonInputStream(readFailureIndex));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public void assertAvroGenerics(Stream<GenericRecord> stream) {
    ListAssert.assertEquals(avroGenerics,
                            stream.collect(Collectors.toList()));
  }

  public void assertAvroSpecifics(Stream<LogEventAvro> stream) {
    ListAssert.assertEquals(avroPojos, stream.collect(Collectors.toList()));
  }

  public InputStream avroBinaryInputStream(int readFailureIndex) {
    return new TestInputStream(avroBinary(), readFailureIndex);
  }
  public InputStream avroJsonInputStream(int readFailureIndex) {
    return new TestInputStream(avroJson().map(String::getBytes), readFailureIndex);
  }

  public InputStream avroBatchInputStream(int readFailureIndex) {
    return new TestInputStream(avroBatch, readFailureIndex);
  }

  public void assertThriftObjects(Stream<LogEventThrift> stream) {
    ListAssert.assertEquals(thriftPojos, stream.collect(Collectors.toList()));
  }

  public InputStream thriftBinaryInputStream(int readFailureIndex) {
    return new TestInputStream(thriftBinary(), readFailureIndex);
  }

  public InputStream thriftCompactInputStream(int readFailureIndex) {
    return new TestInputStream(thriftCompact(), readFailureIndex);
  }

  public InputStream thriftJsonInputStream(int readFailureIndex) {
    return new TestInputStream(thriftJson().map(String::getBytes), readFailureIndex);
  }

  public InputStream protoBinaryInputStream(int readFailureIndex) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    protobuf().forEach(Unchecked.consumer(br -> br.writeDelimitedTo(baos)));
    return new TestInputStream(baos.toByteArray(), readFailureIndex);
  }

  public void assertProtobufObjects(Stream<AdExchangeProtobuf.LogEvent> stream) {
    ListAssert.assertEquals(protobufPojos, stream.collect(Collectors.toList()));
  }
}
