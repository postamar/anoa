package com.adgear.anoa.library.other;

import com.adgear.anoa.Anoa;
import com.adgear.anoa.AnoaHandler;
import com.adgear.anoa.library.write.AvroConsumers;
import com.adgear.anoa.library.write.AvroEncoders;
import com.adgear.anoa.library.write.WriteConsumer;
import com.adgear.anoa.read.AvroDecoders;
import com.adgear.anoa.read.AvroStreams;
import com.adgear.anoa.read.JacksonStreams;
import com.adgear.anoa.test.AnoaTestSample;
import com.adgear.anoa.test.ad_exchange.LogEventAvro;
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

  static final AnoaTestSample ATS = new AnoaTestSample();

  @Test
  public void test() throws Exception {
    AnoaHandler<Throwable> f = AnoaHandler.NO_OP_HANDLER;
    try (InputStream inputStream = ATS.jsonInputStream(-1)) {
      Stream<Anoa<TreeNode, Throwable>> treeNodeStream =
          new JacksonStreams<>(new ObjectMapper(), Optional.<FormatSchema>empty())
              .from(f, inputStream);

      long total = treeNodeStream
          .map(f.function(TreeNode::traverse))
          .map(AvroDecoders.jackson(f, ATS.avroClass, true))
          .map(AvroEncoders.jackson(f,
                                    ATS.avroClass,
                                    () -> new TokenBuffer(AnoaTestSample.OBJECT_MAPPER, false),
                                    false))
          .map(f.function(TokenBuffer::asParser))
          .map(f.functionChecked(JsonParser::readValueAsTree))
          .filter(Anoa::isPresent)
          .count();

      Assert.assertEquals(ATS.nl, total);
    }
  }

  @Test
  public void testFile() throws Exception {
    JsonParser jp = AnoaTestSample.OBJECT_MAPPER.getFactory().createParser(ATS.jsonInputStream(-1));
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (WriteConsumer<LogEventAvro> consumer =
             AvroConsumers.batch(ATS.avroClass, baos)) {
      long total = AvroStreams.jackson(AnoaHandler.NO_OP_HANDLER, ATS.avroClass, true, jp)
          .map(AnoaHandler.NO_OP_HANDLER.writeConsumer(consumer))
          .filter(Anoa::isPresent)
          .count();

      Assert.assertEquals(ATS.n, total);
    }

    Assert.assertEquals(ATS.n,
                        AvroStreams.batch(ATS.avroSchema,
                                          new ByteArrayInputStream(baos.toByteArray())).count());
  }

}
