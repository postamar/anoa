package com.adgear.anoa;

import com.google.openrtb.OpenRtb;
import com.google.openrtb.json.OpenRtbJsonFactory;

import com.adgear.avro.openrtb.BidRequest;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TTransport;
import org.jooq.lambda.Unchecked;
import org.jooq.lambda.fi.util.function.CheckedConsumer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import junitx.framework.ListAssert;

public class BidReqs {

  static public final int n;

  static public final TestResource JSON;
  static public final TestResource AVRO;
  static public final TestResource THRIFT;

  static public final ObjectMapper objectMapper;
  static public final Schema avroSchema;
  static public final Class<com.adgear.avro.openrtb.BidRequest> avroClass;
  static public final Class<thrift.com.adgear.avro.openrtb.BidRequest> thriftClass;
  static public final Supplier<thrift.com.adgear.avro.openrtb.BidRequest> thriftSupplier;
  static public final Class<OpenRtb.BidRequest> protobufClass;
  static protected final List<ObjectNode> jsonObjects;
  static protected final List<GenericRecord> avroGenerics;
  static protected final List<com.adgear.avro.openrtb.BidRequest> avroSpecifics;
  static protected final List<byte[]> avroBinaries;
  static protected final byte[] avroBatch;
  static protected final List<thrift.com.adgear.avro.openrtb.BidRequest> thrifts;
  static protected final List<byte[]> thriftCompacts;
  static protected final List<byte[]> thriftBinaries;
  static protected final List<OpenRtb.BidRequest> protobufs;
  static protected final List<byte[]> protobufBinaries;

  static {
    objectMapper = new ObjectMapper();

    JSON = new TestResource("/bidreqs.json");
    jsonObjects = JSON.jsonBytes()
        .map(Unchecked.function(objectMapper::readTree))
        .map(n -> (ObjectNode) n)
        .collect(Collectors.toList());

    n = jsonObjects.size();

    AVRO = new TestResource("/bidreqs_avro.json");

    avroSchema = com.adgear.avro.openrtb.BidRequest.getClassSchema();
    {
      GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(avroSchema);
      avroGenerics = AVRO.jsonStrings()
          .map((String record) -> {
            try {
              return reader.read(null, DecoderFactory.get().jsonDecoder(avroSchema, record));
            } catch (IOException e) {
              throw new UncheckedIOException(e);
            }
          })
          .collect(Collectors.toList());
    }
    avroClass = com.adgear.avro.openrtb.BidRequest.class;
    {
      SpecificDatumReader<com.adgear.avro.openrtb.BidRequest> reader
          = new SpecificDatumReader<>(com.adgear.avro.openrtb.BidRequest.class);
      avroSpecifics = AVRO.jsonStrings()
          .map((String record) -> {
            try {
              return reader.read(null, DecoderFactory.get().jsonDecoder(avroSchema, record));
            } catch (IOException e) {
              throw new UncheckedIOException(e);
            }
          })
          .collect(Collectors.toList());
    }
    {
      GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(avroSchema);
      avroBinaries = avroGenerics.stream().sequential()
          .map((GenericRecord r) -> {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try {
              writer.write(r, EncoderFactory.get().directBinaryEncoder(baos, null));
            } catch (IOException e) {
              throw new UncheckedIOException(e);
            }
            return baos.toByteArray();
          }).collect(Collectors.toList());
    }
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      final DataFileWriter<GenericRecord> w = new DataFileWriter<>(
          new GenericDatumWriter<GenericRecord>(avroSchema)).create(avroSchema, baos);
      avroGenerics.stream().sequential().forEach(r -> {
        try {
          w.append(r);
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      });
      w.flush();
      avroBatch = baos.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    THRIFT = new TestResource("/bidreqs_thrift.json");
    thriftClass = thrift.com.adgear.avro.openrtb.BidRequest.class;
    thriftSupplier = thrift.com.adgear.avro.openrtb.BidRequest::new;
    thrifts = THRIFT.jsonBytes()
        .map(TMemoryInputTransport::new)
        .map(TJSONProtocol::new)
        .map(tjsonProtocol -> {
          thrift.com.adgear.avro.openrtb.BidRequest br = thriftSupplier.get();
          try {
            br.read(tjsonProtocol);
          } catch (TException e) {
            throw new RuntimeException(e);
          }
          return br;
        }).collect(Collectors.toList());
    thriftCompacts = thrifts.stream().sequential().map(t -> {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      TTransport tTransport = new TIOStreamTransport(baos);
      TProtocol tCompactProtocol = new TCompactProtocol(tTransport);
      try {
        t.write(tCompactProtocol);
        tTransport.flush();
      } catch (TException e) {
        throw new RuntimeException(e);
      }
      return baos.toByteArray();
    }).collect(Collectors.toList());
    thriftBinaries = thrifts.stream().sequential().map(t -> {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      TTransport tTransport = new TIOStreamTransport(baos);
      TProtocol tBinaryProtocol = new TBinaryProtocol(tTransport);
      try {
        t.write(tBinaryProtocol);
        tTransport.flush();
      } catch (TException e) {
        throw new RuntimeException(e);
      }
      return baos.toByteArray();
    }).collect(Collectors.toList());

    protobufClass = OpenRtb.BidRequest.class;

    OpenRtbJsonFactory openRtbJsonFactory = OpenRtbJsonFactory.create();
    protobufs = JSON.jsonStrings()
        .map(Unchecked.function(json -> openRtbJsonFactory.newReader().readBidRequest(json)))
        .collect(Collectors.toList());
    protobufBinaries = protobufs.stream()
        .map(OpenRtb.BidRequest::toByteArray)
        .collect(Collectors.toList());
  }

  static public InputStream allAsStream(CheckedConsumer<OutputStream> lambda) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Unchecked.consumer(lambda).accept(baos);
    return new ByteArrayInputStream(baos.toByteArray());
  }

  static public void assertJsonObjects(Stream<ObjectNode> stream) {
    ListAssert.assertEquals(jsonObjects, stream.collect(Collectors.toList()));
  }

  static public Stream<ObjectNode> jsonObjects() {
    return jsonObjects.stream().sequential();
  }

  static public Stream<String> jsonStrings() {
    return JSON.jsonStrings();
  }

  static public Stream<byte[]> jsonBytes() {
    return JSON.jsonBytes();
  }

  static public InputStream jsonBytes(int readFailureIndex) {
    return new TestInputStream(jsonBytes(), readFailureIndex);
  }

  static public JsonParser jsonParser(int readFailureIndex) {
    try {
      return objectMapper.getFactory().createParser(jsonBytes(readFailureIndex));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public void assertAvroGenerics(Stream<GenericRecord> stream) {
    ListAssert.assertEquals(avroGenerics, stream.collect(Collectors.toList()));
  }

  static public void assertAvroSpecifics(Stream<BidRequest> stream) {
    ListAssert.assertEquals(avroSpecifics, stream.collect(Collectors.toList()));
  }


  static public Stream<GenericRecord> avroGeneric() {
    return avroGenerics.stream().sequential();
  }

  static public Stream<com.adgear.avro.openrtb.BidRequest> avroSpecific() {
    return avroSpecifics.stream().sequential();
  }

  static public Stream<byte[]> avroBinary() {
    return avroBinaries.stream().sequential();
  }

  static public InputStream avroBinary(int readFailureIndex) {
    return new TestInputStream(avroBinary(), readFailureIndex);
  }

  static public Stream<String> avroJson() {
    return AVRO.jsonStrings();
  }

  static public InputStream avroJson(int readFailureIndex) {
    return new TestInputStream(AVRO.jsonBytes(), readFailureIndex);
  }

  static public InputStream avroBatch(int readFailureIndex) {
    return new TestInputStream(avroBatch, readFailureIndex);
  }

  static public void assertThriftObjects(Stream<thrift.com.adgear.avro.openrtb.BidRequest> stream) {
    ListAssert.assertEquals(thrifts, stream.collect(Collectors.toList()));
  }

  static public Stream<thrift.com.adgear.avro.openrtb.BidRequest> thrift() {
    return thrifts.stream().sequential();
  }

  static public Stream<byte[]> thriftBinary() {
    return thriftBinaries.stream().sequential();
  }

  static public InputStream thriftBinary(int readFailureIndex) {
    return new TestInputStream(thriftBinary(), readFailureIndex);
  }

  static public Stream<byte[]> thriftCompact() {
    return thriftCompacts.stream().sequential();
  }

  static public InputStream thriftCompact(int readFailureIndex) {
    return new TestInputStream(thriftCompact(), readFailureIndex);
  }

  static public Stream<byte[]> thriftJson() {
    return THRIFT.jsonBytes();
  }

  static public InputStream thriftJson(int readFailureIndex) {
    return new TestInputStream(thriftJson(), readFailureIndex);
  }

  static public Stream<OpenRtb.BidRequest> protobuf() { return protobufs.stream().sequential(); }

  static public Stream<byte[]> protobufBinary() { return protobufBinaries.stream().sequential(); }

  static public InputStream protobufBinary(int readFailureIndex) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    protobuf().forEach(Unchecked.consumer(br -> br.writeDelimitedTo(baos)));
    return new TestInputStream(baos.toByteArray(), readFailureIndex);
  }

  static public void assertProtobufObjects(Stream<OpenRtb.BidRequest> stream) {
    ListAssert.assertEquals(protobufs, stream.collect(Collectors.toList()));
  }
}
