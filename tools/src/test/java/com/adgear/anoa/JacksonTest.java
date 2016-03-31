package com.adgear.anoa;

import com.adgear.anoa.read.AvroDecoders;
import com.adgear.anoa.read.AvroStreams;
import com.adgear.anoa.read.CborStreams;
import com.adgear.anoa.read.CsvStreams;
import com.adgear.anoa.read.JacksonDecoders;
import com.adgear.anoa.read.JsonStreams;
import com.adgear.anoa.read.SmileStreams;
import com.adgear.anoa.test.AnoaTestSample;
import com.adgear.anoa.test.simple.Simple;
import com.adgear.anoa.test.simple.SimpleAvro;
import com.adgear.anoa.write.AvroConsumers;
import com.adgear.anoa.write.AvroEncoders;
import com.adgear.anoa.write.CborConsumers;
import com.adgear.anoa.write.CsvConsumers;
import com.adgear.anoa.write.JacksonEncoders;
import com.adgear.anoa.write.JsonConsumers;
import com.adgear.anoa.write.SmileConsumers;
import com.adgear.anoa.write.WriteConsumer;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.cbor.CBORGenerator;
import com.fasterxml.jackson.dataformat.csv.CsvGenerator;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.dataformat.smile.SmileGenerator;

import org.apache.commons.codec.binary.Hex;
import org.jooq.lambda.Unchecked;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Scanner;
import java.util.function.Function;
import java.util.stream.Collectors;

import junitx.framework.ListAssert;

public class JacksonTest {

  static AnoaTestSample ATS = new AnoaTestSample();

  @Test
  public void test() throws Exception {
    long n = ATS.json()
        .map(String::getBytes)
        .map(JacksonDecoders.json())
        .map(JacksonEncoders.cbor())
        .map(JacksonDecoders.cbor())
        .map(JacksonEncoders.json())
        .count();
    Assert.assertEquals(ATS.nl, n);
  }

  @Test
  public void testCsv() throws Exception {
    CsvSchema schema = CsvSchema.builder()
        .addColumn("foo").addColumn("bar").addColumn("baz")
        .setColumnSeparator('\t')
        .disableQuoteChar()
        .setUseHeader(true)
        .build();

    InputStream inputStream = getClass().getResourceAsStream("/in/simple.csv");
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

    JsonParser csvParser = CsvStreams.csvWithHeader().parser(inputStream);
    csvParser.setSchema(CsvSchema.builder().setUseHeader(true).build());

    try (CsvGenerator tsvGenerator = new CsvConsumers(schema).generator(outputStream)) {
      AvroStreams.jackson(SimpleAvro.class, false, csvParser)
          .map(AvroEncoders.jackson(SimpleAvro.class, () -> tsvGenerator, true))
          .forEach(Unchecked.consumer(CsvGenerator::flush));
    }

    Assert.assertEquals(new Scanner(getClass().getResourceAsStream("/out/simple.csv"), "UTF-8")
                            .useDelimiter("\\A").next(),
                        outputStream.toString("UTF-8"));
  }

  @Test
  public void testSimple() throws Exception {
    final SimpleAvro simple = SimpleAvro.newBuilder()
        .setFoo(101)
        .setBar(ByteBuffer.wrap(Hex.decodeHex("FEEB".toCharArray())))
        .setBaz(789.1)
        .build();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Function<JsonParser, SimpleAvro> readFn = AvroDecoders.jackson(SimpleAvro.class, true);

    {
      JsonConsumers jsonConsumers = new JsonConsumers();
      JsonStreams jsonStreams = new JsonStreams();
      try (WriteConsumer<ObjectNode> writeConsumer = jsonConsumers.to(baos)) {
        writeConsumer.accept(
            AvroEncoders.jackson(SimpleAvro.class, jsonConsumers::generator, true)
                .apply(simple)
                .asParser(jsonStreams.objectCodec)
                .readValueAsTree());
      }
      System.out.println(baos);
      Assert.assertEquals(simple, readFn.apply(jsonStreams.parser(baos.toByteArray())));
    }
    baos.reset();

    {
      try (CBORGenerator cborGenerator = new CborConsumers().generator(baos)) {
        AvroConsumers.jackson(SimpleAvro.class, cborGenerator, true).accept(simple);
      }
      System.out.println(baos);
      Assert.assertEquals(simple, readFn.apply(new CborStreams().parser(baos.toByteArray())));
    }
    baos.reset();

    {
      try (SmileGenerator smileGenerator = new SmileConsumers().generator(baos)) {
        AvroConsumers.jackson(SimpleAvro.class, smileGenerator, true).accept(simple);
      }
      System.out.println(baos);
      Assert.assertEquals(simple, readFn.apply(new SmileStreams().parser(baos.toByteArray())));
    }
    baos.reset();
  }

  @Test
  public void testLogEvent() throws Exception {
    List<ObjectNode> list = new JsonStreams()
        .from(ATS.jsonInputStream(-1))
        .collect(Collectors.toList());

    Assert.assertEquals(ATS.n, list.size());

    {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try (WriteConsumer<ObjectNode> writeConsumer = new CborConsumers().to(baos)) {
        list.stream().forEach(writeConsumer);
      }
      ListAssert.assertEquals(list, new CborStreams()
          .from(baos.toByteArray())
          .collect(Collectors.toList()));
    }

    {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try (WriteConsumer<ObjectNode> writeConsumer = new SmileConsumers().to(baos)) {
        list.stream().forEach(writeConsumer);
      }
      ListAssert.assertEquals(list, new SmileStreams()
          .from(baos.toByteArray())
          .collect(Collectors.toList()));
    }

    {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try (WriteConsumer<ObjectNode> writeConsumer = new JsonConsumers().to(baos)) {
        list.stream().forEach(writeConsumer);
      }
      ListAssert.assertEquals(list, new JsonStreams()
          .from(baos.toByteArray())
          .collect(Collectors.toList()));
    }
  }
}
