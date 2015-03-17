package com.adgear.anoa.test;

import com.adgear.anoa.Anoa;
import com.adgear.anoa.AnoaFactory;
import com.adgear.anoa.read.AvroDecoders;
import com.adgear.anoa.read.AvroGenericStreams;
import com.adgear.anoa.read.AvroSpecificStreams;
import com.adgear.anoa.read.JacksonStreams;
import com.adgear.anoa.write.AvroConsumers;
import com.adgear.anoa.write.AvroEncoders;
import com.adgear.anoa.write.WriteConsumer;
import com.adgear.avro.openrtb.BidRequest;
import com.fasterxml.jackson.core.FormatSchema;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.TokenBuffer;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.Optional;
import java.util.stream.Stream;


public class AvroTest {

  static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  public void test() throws Exception {
    AnoaFactory<Throwable> f = AnoaFactory.passAlong();
    try (InputStream inputStream = getClass().getResourceAsStream("/bidreqs.json")) {
      Stream<Anoa<TreeNode, Throwable>> treeNodeStream =
          new JacksonStreams<>(new ObjectMapper(), Optional.<FormatSchema>empty())
              .from(f, inputStream);

      long total = treeNodeStream
          .map(f.function(TreeNode::traverse))
          .map(AvroDecoders.jackson(f, BidRequest.class, true))
          .map(AvroEncoders.jackson(f, () -> new TokenBuffer(MAPPER, false), BidRequest.class))
          .map(f.function(TokenBuffer::asParser))
          .map(f.functionChecked(JsonParser::readValueAsTree))
          .peek(System.out::println)
          .filter(Anoa::isPresent)
          .count();

        Assert.assertEquals(946, total);
    }
  }

  @Test
  public void testFile() throws Exception {
    JsonParser jp = MAPPER.getFactory()
        .createParser(getClass().getResourceAsStream("/bidreqs.json"));
    AnoaFactory<Throwable> f = AnoaFactory.passAlong();

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (WriteConsumer<BidRequest, ?> consumer = AvroConsumers.batch(baos, BidRequest.class)) {
      long total = AvroSpecificStreams.jackson(f, jp, BidRequest.class, true)
          .map(f.writeConsumer(consumer))
          .filter(Anoa::isPresent)
          .count();

      Assert.assertEquals(946, total);
    }

    Assert.assertEquals(946, AvroGenericStreams.batch(new ByteArrayInputStream(baos.toByteArray()),
                                                      BidRequest.getClassSchema()).count());
  }
}
