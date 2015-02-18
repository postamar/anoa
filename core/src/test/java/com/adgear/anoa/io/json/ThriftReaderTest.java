package com.adgear.anoa.io.json;

import com.adgear.anoa.AnoaCollector;
import com.adgear.anoa.AnoaFunction;
import com.adgear.anoa.AnoaRecord;
import com.adgear.anoa.AnoaSummary;
import com.adgear.anoa.io.read.json.JsonReader;

import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.nio.channels.Channels;

import thrift.com.adgear.avro.openrtb.BidRequest;

import static org.junit.Assert.assertEquals;

public class ThriftReaderTest {

  @Test
  public void test() throws Exception {

    try (InputStream inputStream = getClass().getResourceAsStream("/bidreqs.json")) {
      AnoaSummary<BidRequest> collected =
          new BufferedReader(Channels.newReader(Channels.newChannel(inputStream), "UTF-8"))
              .lines()
              .map(String::getBytes)
              .map(AnoaRecord::create)
              .map(AnoaFunction.pokemonize(JsonReader::createParser))
              .map(AnoaFunction.pokemonize(JsonReader.lambda(BidRequest.class, true)))
              .collect(AnoaCollector.inList());

      collected.streamCounters().forEach(System.err::println);
      assertEquals(946, collected.streamPresent().filter(BidRequest.class::isInstance).count());
    }
  }

}
