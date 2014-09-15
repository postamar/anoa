package com.adgear.anoa;

import com.google.common.collect.Lists;

import com.adgear.anoa.avro.AvroUtils;
import com.adgear.anoa.codec.avro.JsonNodeToAvro;
import com.adgear.anoa.codec.schemaless.BytesToJsonNode;
import com.adgear.anoa.codec.schemaless.ThriftToJsonBytes;
import com.adgear.anoa.codec.serialized.AvroGenericToBytes;
import com.adgear.anoa.codec.serialized.AvroSpecificToBytes;
import com.adgear.anoa.codec.serialized.ThriftToCompactBytes;
import com.adgear.anoa.codec.thrift.AvroBytesToThrift;
import com.adgear.anoa.provider.IteratorProvider;
import com.adgear.anoa.provider.SingleProvider;
import com.adgear.anoa.provider.avro.SingleAvroProvider;
import com.adgear.anoa.sink.CollectionSink;
import com.adgear.anoa.sink.serialized.BytesSink;
import com.adgear.anoa.source.schemaless.JsonNodeSource;
import com.adgear.anoa.source.thrift.ThriftCompactSource;
import com.adgear.generated.avro.BrowserType;
import com.adgear.generated.avro.Nested2;
import com.adgear.generated.avro.flat.Simple;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class ThriftTest {

  @Test
  @SuppressWarnings("unchecked")
  public void testSimpleToAvro() throws Exception {
    com.adgear.generated.thrift.flat.Simple tSimple =
        new com.adgear.generated.thrift.flat.Simple(654, "a string", 9.11);

    List<Simple> result = new CollectionSink<>(new ArrayList<Simple>())
        .appendAll(new JsonNodeToAvro<>(
            new BytesToJsonNode(
                new ThriftToJsonBytes<>(new SingleProvider<>(tSimple))),
            Simple.class))
        .getCollection();

    assertEquals(1, result.size());

    Simple s = result.get(0);

    assertEquals((Object) tSimple.getFoo(), (Object) s.getFoo());
    assertEquals(tSimple.getBar(), s.getBar().toString());
    assertEquals((Object) tSimple.getBaz(), (Object) s.getBaz());
  }

  @Test
  public void testSimpleFromAvro() throws IOException {
    Simple s = Simple.newBuilder().setFoo(3).setBar("y").setBaz(-.4).build();

    List<com.adgear.generated.thrift.flat.Simple> result =
        new CollectionSink<>(new ArrayList<com.adgear.generated.thrift.flat.Simple>())
            .appendAll(
                new AvroBytesToThrift<>(
                    new AvroSpecificToBytes<>(new SingleAvroProvider<>(s), Simple.class),
                    com.adgear.generated.thrift.flat.Simple.class))
            .getCollection();

    assertEquals(1, result.size());

    com.adgear.generated.thrift.flat.Simple r = result.get(0);

    assertEquals((Object) r.getFoo(), (Object) s.getFoo());
    assertEquals(r.getBar(), s.getBar().toString());
    assertEquals((Object) r.getBaz(), (Object) s.getBaz());
  }

  @Test
  public void testNested2Single() throws IOException {
    com.adgear.generated.thrift.Nested2 n1 = new com.adgear.generated.thrift.Nested2();
    n1.setType(com.adgear.generated.thrift.BrowserType.CHROME);
    n1.setNumbers(Lists.newArrayList(1.7, -20.8, 300.9));
    Map<String, List<com.adgear.generated.thrift.flat.Simple>> map = new HashMap<>();
    com.adgear.generated.thrift.flat.Simple simple =
        new com.adgear.generated.thrift.flat.Simple(9, "a string", -0.7);
    map.put("key", Lists.newArrayList(simple));
    n1.setUgly(map);

    List<Nested2> result = new CollectionSink<>(new ArrayList<Nested2>())
        .appendAll(
            new JsonNodeToAvro<>(
                new BytesToJsonNode(
                    new ThriftToJsonBytes<>(
                        new SingleProvider<>(n1))),
                Nested2.class))
        .getCollection();

    assertEquals(1, result.size());

    Nested2 n2 = result.get(0);
    assertEquals((Object) n2.getType(), BrowserType.CHROME);
    assertEquals(n2.getNumbers().size(), 3);
    assertEquals((double) n2.getNumbers().get(0), 1.7, 0.0001);
    assertEquals((double) n2.getNumbers().get(1), -20.8, 0.0001);
    assertEquals((double) n2.getNumbers().get(2), 300.9, 0.0001);
    assertEquals(n2.getUgly().size(), 1);
    assertTrue(n2.getUgly().containsKey("key"));
    assertEquals(n2.getUgly().get("key").size(), 1);
    assertEquals((int) n2.getUgly().get("key").get(0).getFoo(), 9);
    assertEquals(n2.getUgly().get("key").get(0).getBar().toString(), "a string");
    assertEquals(n2.getUgly().get("key").get(0).getBaz(), -0.7, 0.0001);
  }

  @Test
  public void testNested2Json() throws IOException {
    final Class<com.adgear.generated.thrift.Nested2> thriftClass =
        com.adgear.generated.thrift.Nested2.class;
    final Schema thriftSchema = AvroUtils.getSchema(thriftClass);
    InputStream stream = getClass().getResourceAsStream("/nested2.json");

    List<Nested2> list =
        new CollectionSink<>(new ArrayList<Nested2>())
            .appendAll(
                new JsonNodeToAvro<>(
                    new BytesToJsonNode(
                        new ThriftToJsonBytes<>(
                            new AvroBytesToThrift<>(
                                new AvroGenericToBytes(
                                    new JsonNodeToAvro<GenericRecord>(new JsonNodeSource(stream),
                                                                      thriftSchema)),
                                com.adgear.generated.thrift.Nested2.class))),
                    Nested2.class))
            .getCollection();

    assertEquals(1, list.size());

    Nested2 n = list.get(0);
    assertEquals(n.getType(), BrowserType.CHROME);
    assertEquals(n.getNumbers().size(), 2);
    assertEquals(n.getUgly().size(), 1);
  }

  @Test
  public void testStream() throws IOException {
    final Class<com.adgear.generated.thrift.flat.Simple> thriftClass =
        com.adgear.generated.thrift.flat.Simple.class;

    List<com.adgear.generated.thrift.flat.Simple> in =
        Arrays.asList(new com.adgear.generated.thrift.flat.Simple(1, "aaa", -8.0),
                      new com.adgear.generated.thrift.flat.Simple(2, "bb", -9.0));

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    new BytesSink(baos)
        .appendAll(
            new ThriftToCompactBytes<>(
                new IteratorProvider<>(in.iterator())))
        .close();

    List<com.adgear.generated.thrift.flat.Simple> out = new CollectionSink<>(
        new ArrayList<com.adgear.generated.thrift.flat.Simple>())
        .appendAll(
            new ThriftCompactSource<>(new ByteArrayInputStream(baos.toByteArray()), thriftClass))
        .getCollection();

    assertEquals(2, out.size());
    assertEquals(1, out.get(0).getFoo());
    assertEquals(2, out.get(1).getFoo());
  }
}
