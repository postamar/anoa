package com.adgear.anoa;

import com.google.common.io.Resources;

import com.adgear.anoa.avro.AvroUtils;
import com.adgear.anoa.codec.avro.BytesToAvroGeneric;
import com.adgear.anoa.codec.avro.BytesToAvroSpecific;
import com.adgear.anoa.codec.avro.JsonNodeToAvro;
import com.adgear.anoa.codec.avro.StringListToAvro;
import com.adgear.anoa.codec.schemaless.AvroSpecificToStringList;
import com.adgear.anoa.codec.serialized.AvroGenericToBytes;
import com.adgear.anoa.codec.serialized.AvroSpecificToBytes;
import com.adgear.anoa.provider.IteratorProvider;
import com.adgear.anoa.sink.CollectionSink;
import com.adgear.anoa.sink.avro.AvroSink;
import com.adgear.anoa.sink.schemaless.CsvSink;
import com.adgear.anoa.source.avro.AvroSpecificSource;
import com.adgear.anoa.source.schemaless.CsvSource;
import com.adgear.anoa.source.schemaless.JsonNodeSource;
import com.adgear.generated.avro.Nested;
import com.adgear.generated.avro.flat.Composite;
import com.adgear.generated.avro.flat.Simple;

import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class MoreTest {

  @Test
  public void testSchema() {
    assertEquals(Nested.getClassSchema(), AvroUtils.getSchema(Nested.class));
  }

  @Test
  public void testClass() throws IOException {
    assertEquals(Nested.class, AvroUtils.getSpecificClass(new Nested().getSchema()));
  }

  @Test
  public void testJsonToListNested() throws IOException {

    List<Nested> in = new ArrayList<>();
    new CollectionSink<>(in).appendAll(
        new JsonNodeToAvro<>(
            new JsonNodeSource(DeserializerTest.streamResource("/nested.json")),
            Nested.class));

    List<Nested> out =
        new CollectionSink<>(new ArrayList<Nested>())
            .appendAll(
                new BytesToAvroSpecific<Nested>(
                    new AvroGenericToBytes(
                        new BytesToAvroGeneric(
                            new AvroSpecificToBytes<>(new IteratorProvider<>(in.iterator()),
                                                      Nested.class)))))
            .getCollection();

    assertEquals(1, in.size());
    assertEquals(in.toString(), out.toString());
  }

  @Test
  public void testJsonToAvroToJsonToListComposite() throws IOException {
    List<Composite> in = new CollectionSink<>(new ArrayList<Composite>())
        .appendAll(
            new JsonNodeToAvro<>(
                new JsonNodeSource(DeserializerTest.streamResource("/composite.json")),
                Composite.class))
        .getCollection();

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    new AvroSink<GenericRecord>(baos, Composite.getClassSchema())
        .appendAll(
            new BytesToAvroGeneric(
                new AvroSpecificToBytes<>(
                    new IteratorProvider<>(in.iterator()),
                    Composite.class)))
        .flush();

    List<Composite> out = new CollectionSink<>(new ArrayList<Composite>())
        .appendAll(
            new AvroSpecificSource<>(new ByteArrayInputStream(baos.toByteArray()), Composite.class))
        .getCollection();

    assertEquals(2, in.size());
    assertEquals(2, out.size());
    assertEquals(in.get(0).toString(), out.get(0).toString());
    assertEquals(in.get(1).toString(), out.get(1).toString());
  }

  @Test
  public void testCsvToAvroToCsvSimple() throws IOException {
    String in = Resources.toString(
        Resources.getResource(MoreTest.class, "/simple_no_header.csv"),
        Charset.forName("UTF-8"))
        .replace("\n", "\r\n");

    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    new CsvSink(stream).appendAll(
        new AvroSpecificToStringList<>(new StringListToAvro<>(
            new CsvSource(DeserializerTest.readResource("/simple_no_header.csv")),
            Simple.class),
                                       Simple.class)).flush();
    String out = stream.toString("UTF-8");

    assertEquals(in, out);
  }
}
