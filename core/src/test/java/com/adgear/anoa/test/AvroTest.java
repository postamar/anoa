package com.adgear.anoa.test;

import com.adgear.anoa.AnoaCollector;
import com.adgear.anoa.AnoaFunction;
import com.adgear.anoa.AnoaRecord;
import com.adgear.anoa.PresentCounted;
import com.adgear.anoa.factory.AvroConsumers;
import com.adgear.anoa.factory.AvroDecoders;
import com.adgear.anoa.factory.AvroEncoders;
import com.adgear.anoa.factory.AvroGenericStreams;
import com.adgear.anoa.factory.JacksonFactory;
import com.adgear.anoa.factory.util.WriteConsumer;
import com.adgear.anoa.read.AnoaRead;
import com.adgear.anoa.write.AnoaWrite;
import com.adgear.avro.openrtb.BidRequest;
import com.fasterxml.jackson.core.FormatSchema;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.TokenBuffer;

import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

public class AvroTest {

  static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  public void test() throws Exception {
    try (InputStream inputStream = getClass().getResourceAsStream("/bidreqs.json")) {
      long total = new JacksonFactory<>(new ObjectMapper(), Optional.<FormatSchema>empty())
          .from(inputStream)
          .map(TreeNode::traverse)
          .map(p -> AnoaRead.biFn(BidRequest.class).apply(p, true))
          .map(AvroEncoders.json(new SpecificDatumWriter<>(BidRequest.class),
                                 BidRequest.getClassSchema()))
          .map(AvroDecoders.json(new GenericDatumReader<GenericRecord>(BidRequest.getClassSchema())))
          .map(AnoaWrite.fn(BidRequest.getClassSchema(), () -> new TokenBuffer(MAPPER, false)))
          .map(TokenBuffer::asParser)
          .map(AnoaRecord::of)
          .map(AnoaFunction.pokemonChecked(JsonParser::readValueAsTree, JsonParser.class))
          .collect(AnoaCollector.toSet())
          .streamCounters()
          .parallel()
          .filter(e -> PresentCounted.is(e.getKey()))
          .findAny()
          .map(Map.Entry::getValue)
          .get();

        Assert.assertEquals(946, total);
    }
  }

  @Test
  public void testFile() throws Exception {
    JsonParser jp = MAPPER.getFactory()
        .createParser(getClass().getResourceAsStream("/bidreqs.json"));

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (WriteConsumer<BidRequest> consumer = AvroConsumers.batch(baos, BidRequest.class)) {
      Stream.generate(() -> true).limit(1000)
          .sequential()
          .map(AnoaRecord::of)
          .map(AnoaFunction.pokemonChecked(b -> AnoaRead.biFn(BidRequest.class).apply(jp, b),
                                           AvroTest.class))
          .collect(AnoaCollector.toList())
          .streamPresent()
          .forEach(consumer);
    }

    Assert.assertEquals(946, AvroGenericStreams.batch(new ByteArrayInputStream(baos.toByteArray()),
                                                      BidRequest.getClassSchema()).count());
  }
}
