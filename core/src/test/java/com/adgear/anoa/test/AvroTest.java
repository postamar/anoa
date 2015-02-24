package com.adgear.anoa.test;

import com.adgear.anoa.AnoaAvro;
import com.adgear.anoa.AnoaCollector;
import com.adgear.anoa.AnoaFunction;
import com.adgear.anoa.AnoaRecord;
import com.adgear.anoa.PresentCounted;
import com.adgear.anoa.read.AnoaRead;
import com.adgear.anoa.write.AnoaWrite;
import com.adgear.avro.openrtb.BidRequest;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.TokenBuffer;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificDatumWriter;
import org.jooq.lambda.Unchecked;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.Map;
import java.util.stream.Stream;

public class AvroTest {

  static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  public void test() throws Exception {
    try (InputStream inputStream = getClass().getResourceAsStream("/bidreqs.json")) {
      try (JsonParser jsonParser = new JsonFactory().createParser(inputStream)) {
        long total = Stream.generate(() -> jsonParser).limit(946)
            .map(p -> AnoaRead.biFn(BidRequest.class).apply(p, true))
            .map(AnoaAvro.toJsonFn(new SpecificDatumWriter<>(BidRequest.class),
                                   BidRequest.getClassSchema()))
            .peek(System.out::println)
            .map(AnoaAvro.fromJsonFn(new GenericDatumReader<>(BidRequest.getClassSchema()),
                                     () -> (GenericRecord) GenericData.get()
                                         .newRecord(null, BidRequest.getClassSchema())))
            .map(AnoaWrite.fn(BidRequest.getClassSchema(), () -> new TokenBuffer(MAPPER, false)))
            .map(TokenBuffer::asParser)
            .map(AnoaRecord::of)
            .map(AnoaFunction.pokemonizeChecked(JsonParser::readValueAsTree, JsonParser.class))
            .collect(AnoaCollector.toSet())
            .streamCounters()
            .parallel()
            .peek(System.err::println)
            .filter(e -> PresentCounted.is(e.getKey()))
            .findAny()
            .map(Map.Entry::getValue)
            .get();
        Assert.assertEquals(946, total);
      }
    }
  }

  @Test
  public void testFile() throws Exception {
    JsonParser jp = MAPPER.getFactory()
        .createParser(getClass().getResourceAsStream("/bidreqs.json"));

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataFileWriter<BidRequest> dataFileWriter = AnoaAvro.to(baos, BidRequest.class);
    Stream.generate(() -> true).limit(1000)
        .sequential()
        .map(AnoaRecord::of)
        .map(AnoaFunction.pokemonizeChecked(b -> AnoaRead.biFn(BidRequest.class).apply(jp, b),
                                            AvroTest.class))
        .collect(AnoaCollector.toList())
        .streamPresent()
        .forEach(Unchecked.consumer(dataFileWriter::append));
    dataFileWriter.flush();

    Assert.assertEquals(946, AnoaAvro.from(new ByteArrayInputStream(baos.toByteArray()),
                                           BidRequest.getClassSchema()).count());
  }
}
