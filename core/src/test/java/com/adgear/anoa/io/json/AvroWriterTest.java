package com.adgear.anoa.io.json;

import com.adgear.anoa.AnoaFunction;
import com.adgear.anoa.AnoaRecord;
import com.adgear.anoa.io.read.json.JsonReader;
import com.adgear.anoa.io.write.json.JsonWriter;
import com.adgear.avro.openrtb.BidRequest;
import com.fasterxml.jackson.databind.util.TokenBuffer;

import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.channels.Channels;

public class AvroWriterTest {

  @Test
  public void test() throws Exception {

    try(InputStream inputStream = getClass().getResourceAsStream("/bidreqs.json")) {
      new BufferedReader(Channels.newReader(Channels.newChannel(inputStream), "UTF-8"))
          .lines()
          .map(String::getBytes)
          .map(AnoaRecord::create)
          .map(AnoaFunction.pokemonize(JsonReader::createParser))
          .map(AnoaFunction.pokemonize(JsonReader.lambda(BidRequest.class, true)))
          .map(AnoaFunction.of(x -> (GenericRecord) x))
          .map(AnoaFunction.pokemonize(JsonWriter.lambda(BidRequest.getClassSchema())))
          .flatMap(AnoaRecord::asStream)
          .map(TokenBuffer::asParser)
          .map(jp -> {
            try {
              return jp.readValueAsTree();
            } catch (IOException e) {
              throw new UncheckedIOException(e);
            }
          })
          .forEach(System.err::println);
    }
  }
}
