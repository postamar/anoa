package com.adgear.anoa;

import com.adgear.anoa.factory.CborObjects;
import com.adgear.anoa.factory.CsvObjects;
import com.adgear.anoa.factory.JsonObjects;
import com.adgear.anoa.factory.SmileObjects;
import com.adgear.anoa.factory.XmlObjects;
import com.adgear.anoa.factory.YamlObjects;
import com.adgear.anoa.factory.util.WriteConsumer;
import com.adgear.anoa.read.AnoaRead;
import com.adgear.anoa.write.AnoaWrite;
import com.adgear.avro.Simple;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.util.TokenBuffer;
import com.fasterxml.jackson.dataformat.cbor.CBORGenerator;
import com.fasterxml.jackson.dataformat.csv.CsvGenerator;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.dataformat.smile.SmileGenerator;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;

import org.apache.commons.codec.binary.Hex;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Scanner;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import junitx.framework.ListAssert;

public class JacksonTest {

  @Test
  public void testCsv() throws Exception {
    BiFunction<JsonParser, Boolean, Simple> readBiFn = AnoaRead.biFn(Simple.class);
    BiConsumer<Simple, JsonGenerator> writeBiCo = AnoaWrite.biCo(Simple.class);
    CsvSchema schema = CsvSchema.builder()
        .addColumn("foo").addColumn("bar").addColumn("baz")
        .setColumnSeparator('\t')
        .setUseHeader(true)
        .build();

    InputStream inputStream = getClass().getResourceAsStream("/in/simple.csv");
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

    JsonParser csvParser = CsvObjects.csvWithHeader().parser(inputStream);
    csvParser.setSchema(CsvSchema.builder().setUseHeader(true).build());

    CsvGenerator tsvGenerator = new CsvObjects(schema).generator(outputStream);
    Stream.of(false, false)
        .sequential()
        .map(b -> readBiFn.apply(csvParser, b))
        .forEach(s -> writeBiCo.accept(s, tsvGenerator));
    tsvGenerator.flush();

    Assert.assertEquals(new Scanner(getClass().getResourceAsStream("/out/simple.csv"), "UTF-8")
                            .useDelimiter("\\A").next(),
                        outputStream.toString("UTF-8"));
  }

  @Test
  public void testSimple() throws Exception {
    final Simple simple = Simple.newBuilder()
        .setFoo(101)
        .setBar(ByteBuffer.wrap(Hex.decodeHex("FEEB".toCharArray())))
        .setBaz(789.1)
        .build();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    BiConsumer<Simple, JsonGenerator> writer = AnoaWrite.biCo(Simple.class);
    Function<JsonParser, Simple> reader = AnoaRead.fn(Simple.class, false);

    {
      XmlObjects xmlObjects = new XmlObjects();
      try (WriteConsumer<ObjectNode> writeConsumer = xmlObjects.to(baos)) {
        TokenBuffer tokenBuffer = xmlObjects.buffer();
        writer.accept(simple, tokenBuffer);
        writeConsumer.accept(tokenBuffer.asParser().readValueAsTree());
      }
      System.out.println(baos);
      Assert.assertEquals(simple, reader.apply(xmlObjects.parser(baos.toByteArray())));
    }
    baos.reset();

    {
      CborObjects cborObjects = new CborObjects();
      try (CBORGenerator cborGenerator = cborObjects.generator(baos)) {
        writer.accept(simple, cborGenerator);
      }
      System.out.println(baos);
      Assert.assertEquals(simple, reader.apply(cborObjects.parser(baos.toByteArray())));
    }
    baos.reset();

    {
      SmileObjects smileObjects = new SmileObjects();
      try (SmileGenerator smileGenerator = smileObjects.generator(baos)) {
        writer.accept(simple, smileGenerator);
      }
      System.out.println(baos);
      Assert.assertEquals(simple, reader.apply(smileObjects.parser(baos.toByteArray())));
    }
    baos.reset();

    {
      YamlObjects yamlObjects = new YamlObjects();
      try (YAMLGenerator yamlGenerator = yamlObjects.generator(baos)) {
        writer.accept(simple, yamlGenerator);
      }
      System.out.println(baos);
      Assert.assertEquals(simple, reader.apply(yamlObjects.parser(baos.toByteArray())));
    }
    baos.reset();

    {
      JsonObjects jsonObjects = new JsonObjects();
      try (JsonGenerator jsonGenerator = jsonObjects.generator(baos)) {
        writer.accept(simple, jsonGenerator);
      }
      System.out.println(baos);
      Assert.assertEquals(simple, reader.apply(jsonObjects.parser(baos.toByteArray())));
    }
  }

  @Test
  public void testBidRequest() throws Exception {
    List<ObjectNode> list = new JsonObjects()
        .from(getClass().getResourceAsStream("/bidreqs.json"))
        .collect(Collectors.toList());

    Assert.assertEquals(946, list.size());

    {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try (WriteConsumer<ObjectNode> writeConsumer = new CborObjects().to(baos)) {
        list.stream().forEach(writeConsumer);
      }
      ListAssert.assertEquals(list, new CborObjects()
          .from(baos.toByteArray())
          .collect(Collectors.toList()));
    }
/*
    {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try (WriteConsumer<ObjectNode> writeConsumer = new XmlObjects().to(baos)) {
        list.stream().forEach(writeConsumer);
      }
      ListAssert.assertEquals(list, new XmlObjects()
          .from(baos.toByteArray())
          .collect(Collectors.toList()));
    }
*/
    {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try (WriteConsumer<ObjectNode> writeConsumer = new SmileObjects().to(baos)) {
        list.stream().forEach(writeConsumer);
      }
      ListAssert.assertEquals(list, new SmileObjects()
          .from(baos.toByteArray())
          .collect(Collectors.toList()));
    }
/*
    {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try (WriteConsumer<ObjectNode> writeConsumer = new YamlObjects().to(baos)) {
        list.stream().forEach(writeConsumer);
      }
      System.err.println(baos.toString());
      ListAssert.assertEquals(list, new YamlObjects()
          .from(baos.toByteArray())
          .collect(Collectors.toList()));
    }
*/
    {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try (WriteConsumer<ObjectNode> writeConsumer = new JsonObjects().to(baos)) {
        list.stream().forEach(writeConsumer);
      }
      ListAssert.assertEquals(list, new JsonObjects()
          .from(baos.toByteArray())
          .collect(Collectors.toList()));
    }
  }
}
