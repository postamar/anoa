package com.adgear.anoa.test;

import com.google.protobuf.ExtensionRegistry;

import com.adgear.anoa.test.ad_exchange.AdExchangeProtobuf;
import com.adgear.anoa.test.ad_exchange.LogEventAvro;
import com.adgear.anoa.test.ad_exchange.LogEventThrift;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.googlecode.protobuf.format.JsonFormat;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.jooq.lambda.Unchecked;
import org.jooq.lambda.fi.util.function.CheckedConsumer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import junitx.framework.ListAssert;

public class AnoaTestSample {

  /* RESOURCES */

  static private final JsonTestResource JSON = new JsonTestResource("/json.json");
  static private final JsonTestResource AVRO = new JsonTestResource("/avro.json");
  static private final JsonTestResource THRIFT = new JsonTestResource("/thrift.json");
  static private final JsonTestResource PROTOBUF = new JsonTestResource("/protobuf.json");


  /* PROPERTIES */

  public final int n = 1000;
  public final long nl = n;

  public final Schema avroSchema = LogEventAvro.getClassSchema();
  public final Class<LogEventAvro> avroClass = LogEventAvro.class;

  protected final List<LogEventAvro> avroPojos = new ArrayList<>();
  protected final List<byte[]> avroBinaries = new ArrayList<>();
  protected final byte[] avroBatch;
  protected final List<GenericRecord> avroGenerics = new ArrayList<>();

  public final Class<LogEventThrift> thriftClass = LogEventThrift.class;
  public final Supplier<LogEventThrift> thriftSupplier = LogEventThrift::new;

  protected final List<LogEventThrift> thriftPojos = new ArrayList<>();
  protected final List<byte[]> thriftBinaries = new ArrayList<>();
  protected final List<byte[]> thriftCompacts = new ArrayList<>();

  public final Class<AdExchangeProtobuf.LogEvent> protobufClass =
      AdExchangeProtobuf.LogEvent.class;
  public final Supplier<AdExchangeProtobuf.LogEvent.Builder> protobufSupplier =
      AdExchangeProtobuf.LogEvent::newBuilder;

  protected final List<AdExchangeProtobuf.LogEvent> protobufPojos = new ArrayList<>();
  protected final List<byte[]> protobufBinaries = new ArrayList<>();


  /* CONSTRUCTOR */

  public AnoaTestSample() {
    try {
      avroBatch = buildAvro();
      buildThrift();
      buildProtobuf();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private byte[] buildAvro() throws IOException {
    DatumReader<LogEventAvro> specificReader = new SpecificDatumReader<>(avroClass);
    for (String json : AVRO.jsonStrings) {
      Decoder decoder = DecoderFactory.get().jsonDecoder(avroSchema, json);
      avroPojos.add(specificReader.read(null, decoder).freeze());
    }
    ByteArrayOutputStream osBatch = new ByteArrayOutputStream();
    DatumWriter<LogEventAvro> specificWriter = new SpecificDatumWriter<>(avroClass);
    try (DataFileWriter<LogEventAvro> fileWriter =
             new DataFileWriter<>(specificWriter).create(avroSchema, osBatch)) {
      for (LogEventAvro avro : avroPojos) {
        fileWriter.append(avro);
        ByteArrayOutputStream osBinary = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().directBinaryEncoder(osBinary, null);
        specificWriter.write(avro, encoder);
        avroBinaries.add(osBinary.toByteArray());
      }
    }
    final byte[] avroBatch = osBatch.toByteArray();
    for (GenericRecord generic : new DataFileStream<>(new ByteArrayInputStream(avroBatch),
                                                      new GenericDatumReader<GenericRecord>())) {
        avroGenerics.add(generic);
    }
    return avroBatch;
  }

  private void buildThrift() throws TException {
    TTestMemoryOutputTransport out = new TTestMemoryOutputTransport();
    for (byte[] json : THRIFT.jsonBytes) {
      LogEventThrift event = thriftSupplier.get();
      event.read(new TJSONProtocol(new TMemoryInputTransport(json)));
      thriftPojos.add(event);
      out.baos.reset();
      event.write(new TBinaryProtocol(out));
      thriftBinaries.add(out.baos.toByteArray());
      out.baos.reset();
      event.write(new TCompactProtocol(out));
      thriftCompacts.add(out.baos.toByteArray());
    }
  }

  private void buildProtobuf() throws IOException {
    JsonFormat jsonFormat = new JsonFormat();
    for (String json : PROTOBUF.jsonStrings) {
      AdExchangeProtobuf.LogEvent.Builder builder = protobufSupplier.get();
      jsonFormat.merge(json, ExtensionRegistry.getEmptyRegistry(), builder);
      AdExchangeProtobuf.LogEvent event = builder.build();
      protobufPojos.add(event);
      protobufBinaries.add(event.toByteArray());
    }
  }

  /* JSON */

  public Stream<String> json() {
    return JSON.strings();
  }


  /* AVRO */

  public Stream<LogEventAvro> avroSpecific() {
    return avroPojos.stream().sequential();
  }

  public Stream<GenericRecord> avroGeneric() {
    return avroGenerics.stream().sequential();
  }

  public Stream<byte[]> avroBinary() {
    return avroBinaries.stream().sequential();
  }

  public Stream<String> avroJson() {
    return AVRO.strings();
  }

  public InputStream avroBatch() {
    return new ByteArrayInputStream(avroBatch);
  }


  /* THRIFT */

  public Stream<LogEventThrift> thrift() {
    return thriftPojos.stream().sequential();
  }

  public Stream<byte[]> thriftBinary() {
    return thriftBinaries.stream().sequential();
  }

  public Stream<byte[]> thriftCompact() {
    return thriftCompacts.stream().sequential();
  }

  public Stream<String> thriftJson() {
    return THRIFT.strings();
  }


  /* PROTOBUF */

  public Stream<AdExchangeProtobuf.LogEvent> protobuf() {
    return protobufPojos.stream().sequential();
  }

  public Stream<byte[]> protobufBinary() {
    return protobufBinaries.stream().sequential();
  }

  public Stream<String> protobufJson() {
    return PROTOBUF.strings();
  }


  /* */

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
