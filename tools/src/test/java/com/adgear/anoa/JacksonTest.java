package com.adgear.anoa;

import com.adgear.anoa.read.AnoaRead;
import com.adgear.anoa.write.AnoaWrite;
import com.adgear.avro.Simple;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.dataformat.cbor.CBORGenerator;
import com.fasterxml.jackson.dataformat.cbor.CBORParser;
import com.fasterxml.jackson.dataformat.csv.CsvGenerator;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.dataformat.smile.SmileGenerator;
import com.fasterxml.jackson.dataformat.smile.SmileParser;
import com.fasterxml.jackson.dataformat.xml.deser.FromXmlParser;
import com.fasterxml.jackson.dataformat.xml.ser.ToXmlGenerator;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.dataformat.yaml.YAMLParser;

import org.apache.commons.codec.binary.Hex;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Scanner;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import javax.xml.namespace.QName;

public class JacksonTest {


  @Test
  public void testCsv() throws Exception {
    BiFunction<JsonParser,Boolean,Simple> readBiFn = AnoaRead.biFn(Simple.class);
    BiConsumer<Simple, JsonGenerator> writeBiCo = AnoaWrite.biCo(Simple.class);

    InputStream inputStream = getClass().getResourceAsStream("/in/simple.csv");
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

    JsonParser csvParser = AnoaJackson.CSV.from(inputStream);
    csvParser.setSchema(CsvSchema.builder().setUseHeader(true).build());
    CsvGenerator tsvGenerator = AnoaJackson.CSV.to(outputStream);
    tsvGenerator.setSchema(CsvSchema.builder()
                               .addColumn("foo").addColumn("bar").addColumn("baz")
                               .setColumnSeparator('\t')
                               .setUseHeader(true)
                               .build());

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
  public void testAll() throws Exception {
    final Simple simple = Simple.newBuilder()
        .setFoo(101)
        .setBar(ByteBuffer.wrap(Hex.decodeHex("FEEB".toCharArray())))
        .setBaz(789.1)
        .build();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    {
      ToXmlGenerator xmlGenerator = AnoaJackson.XML.to(baos);
      xmlGenerator.setNextName(new QName("simple"));
      AnoaWrite.biCo(Simple.class).accept(simple, xmlGenerator);
      xmlGenerator.flush();
    }
    {
      System.out.println(baos);
      FromXmlParser xmlParser = AnoaJackson.XML.from(baos.toByteArray());
      Simple xmlSimple = AnoaRead.fn(Simple.class, false).apply(xmlParser);
      Assert.assertEquals(simple, xmlSimple);
    }
    baos.reset();

    {
      CBORGenerator cborGenerator = AnoaJackson.CBOR.to(baos);
      AnoaWrite.biCo(Simple.class).accept(simple, cborGenerator);
      cborGenerator.flush();
    }
    {
      System.out.println(baos);
      CBORParser cborParser = AnoaJackson.CBOR.from(baos.toByteArray());
      Simple cborSimple = AnoaRead.fn(Simple.class, false).apply(cborParser);
      Assert.assertEquals(simple, cborSimple);
    }
    baos.reset();

    {
      SmileGenerator smileGenerator = AnoaJackson.SMILE.to(baos);
      AnoaWrite.biCo(Simple.class).accept(simple, smileGenerator);
      smileGenerator.flush();
    }
    {
      System.out.println(baos);
      SmileParser smileParser = AnoaJackson.SMILE.from(baos.toByteArray());
      Simple smileSimple = AnoaRead.fn(Simple.class, false).apply(smileParser);
      Assert.assertEquals(simple, smileSimple);
    }
    baos.reset();

    {
      YAMLGenerator yamlGenerator = AnoaJackson.YAML.to(baos);
      AnoaWrite.biCo(Simple.class).accept(simple, yamlGenerator);
      yamlGenerator.flush();
    }
    {
      System.out.println(baos);
      YAMLParser yamlParser = AnoaJackson.YAML.from(baos.toByteArray());
      Simple yamlSimple = AnoaRead.fn(Simple.class, false).apply(yamlParser);
      Assert.assertEquals(simple, yamlSimple);
    }
    baos.reset();

    {
      JsonGenerator jsonGenerator = AnoaJackson.JSON.to(baos);
      AnoaWrite.biCo(Simple.class).accept(simple, jsonGenerator);
      jsonGenerator.flush();
    }
    {
      System.out.println(baos);
      JsonParser jsonParser = AnoaJackson.YAML.from(baos.toByteArray());
      Simple jsonSimple = AnoaRead.fn(Simple.class, false).apply(jsonParser);
      Assert.assertEquals(simple, jsonSimple);
    }
    baos.reset();
  }
}
