package com.adgear.anoa.test;

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

import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.util.Map;
import java.util.stream.Stream;

public class AvroWriterTest {

  static public final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Test
  public void test() throws Exception {


    try (InputStream inputStream = getClass().getResourceAsStream("/bidreqs.json")) {
      try (JsonParser jsonParser = new JsonFactory().createParser(inputStream)) {
        long total = Stream.generate(() -> jsonParser).limit(946)
            .map(p -> AnoaRead.biFn(BidRequest.class)
                .andThen(br -> (GenericRecord) br)
                .apply(p, true))
            .map(AnoaWrite.fn(BidRequest.getClassSchema(),
                              () -> new TokenBuffer(OBJECT_MAPPER, false)))
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
}
