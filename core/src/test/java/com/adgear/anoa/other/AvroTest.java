package com.adgear.anoa.other;

import com.adgear.anoa.Anoa;
import com.adgear.anoa.AnoaHandler;
import com.adgear.anoa.AnoaJacksonTypeException;
import com.adgear.anoa.BidReqs;
import com.adgear.anoa.read.AvroDecoders;
import com.adgear.anoa.read.AvroStreams;
import com.adgear.anoa.read.JacksonStreams;
import com.adgear.anoa.write.AvroConsumers;
import com.adgear.anoa.write.AvroEncoders;
import com.adgear.anoa.write.WriteConsumer;
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
/*
  @Test(expected = AnoaJacksonTypeException.class)
  public void testMissingFields() throws Exception {
    AvroStreams.jackson(Simple.class, false, MAPPER.getFactory().createParser("{\"baz\":1.9}"))
        .forEach(System.err::println);
  }
*/
  @Test
  public void test() throws Exception {
    AnoaHandler<Throwable> f = AnoaHandler.NO_OP_HANDLER;
    try (InputStream inputStream = getClass().getResourceAsStream("/bidreqs.json")) {
      Stream<Anoa<TreeNode, Throwable>> treeNodeStream =
          new JacksonStreams<>(new ObjectMapper(), Optional.<FormatSchema>empty())
              .from(f, inputStream);

      long total = treeNodeStream
          .map(f.function(TreeNode::traverse))
          .map(AvroDecoders.jackson(f, BidReqs.avroClass, true))
          .map(AvroEncoders.jackson(f,  BidReqs.avroClass, () -> new TokenBuffer(MAPPER, false)))
          .map(f.function(TokenBuffer::asParser))
          .map(f.functionChecked(JsonParser::readValueAsTree))
          .filter(Anoa::isPresent)
          .count();

      Assert.assertEquals(946, total);
    }
  }

  @Test
  public void testFile() throws Exception {
    JsonParser jp = MAPPER.getFactory()
        .createParser(getClass().getResourceAsStream("/bidreqs.json"));

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (WriteConsumer<open_rtb.BidRequestAvro> consumer =
             AvroConsumers.batch(BidReqs.avroClass, baos)) {
      long total = AvroStreams.jackson(AnoaHandler.NO_OP_HANDLER, BidReqs.avroClass, true, jp)
          .map(AnoaHandler.NO_OP_HANDLER.writeConsumer(consumer))
          .filter(Anoa::isPresent)
          .count();

      Assert.assertEquals(946, total);
    }

    Assert.assertEquals(946, AvroStreams
        .batch( BidReqs.avroSchema, new ByteArrayInputStream(baos.toByteArray())
        ).count());
  }

}
