package com.adgear.anoa;

import com.google.common.collect.Lists;

import com.adgear.anoa.codec.Codec;
import com.adgear.anoa.codec.avro.JsonNodeToAvro;
import com.adgear.anoa.codec.avro.StringListToAvro;
import com.adgear.anoa.codec.avro.ValueToAvro;
import com.adgear.anoa.codec.base.JsonNodeDeserializerBase;
import com.adgear.anoa.codec.schemaless.AvroGenericToValue;
import com.adgear.anoa.codec.schemaless.AvroSpecificToStringList;
import com.adgear.anoa.codec.schemaless.AvroSpecificToValue;
import com.adgear.anoa.codec.schemaless.BytesToJsonNode;
import com.adgear.anoa.codec.serialized.ValueToJsonBytes;
import com.adgear.anoa.provider.Provider;
import com.adgear.anoa.provider.SingleProvider;
import com.adgear.anoa.provider.avro.AvroProvider;
import com.adgear.anoa.sink.CollectionSink;
import com.adgear.anoa.sink.schemaless.CsvSink;
import com.adgear.anoa.sink.serialized.BytesLineSink;
import com.adgear.anoa.source.schemaless.CsvWithHeaderSource;
import com.adgear.anoa.source.schemaless.JsonNodeSource;
import com.adgear.anoa.source.schemaless.TsvSource;
import com.adgear.anoa.source.serialized.BytesLineSource;
import com.adgear.generated.avro.BrowserType;
import com.adgear.generated.avro.Nested;
import com.adgear.generated.avro.flat.Composite;
import com.adgear.generated.avro.flat.Simple;
import com.fasterxml.jackson.databind.JsonNode;

import org.apache.avro.generic.GenericRecord;
import org.junit.Test;
import org.msgpack.type.Value;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class DeserializerTest {

  public static Reader readResource(String resourcePath) {
    return new InputStreamReader(streamResource(resourcePath), Charset.forName("UTF-8"));
  }

  public static InputStream streamResource(String resourcePath) {
    return DeserializerTest.class.getResourceAsStream(resourcePath);
  }

  @Test
  public void testEmptyJsonBytes() throws IOException {
    Provider<JsonNode> provider = new BytesToJsonNode(
        new BytesLineSource(new ByteArrayInputStream("\n".getBytes())));
    List<JsonNode> list = new CollectionSink<>(new ArrayList<JsonNode>())
        .appendAll(provider)
        .getCollection();
    assertTrue(list.isEmpty());
    assertEquals(2, provider.getCountDropped());
    assertEquals(2, provider.getCounters()
        .get(JsonNodeDeserializerBase.Counter.EMPTY_RECORD.toString()).longValue());
  }

  @Test
  public void testSimpleSerialization() throws IOException {
    Simple begin = Simple.newBuilder().setFoo(1).setBar("te,s\"t").setBaz(-1.1).build();

    Provider<Simple> source = new SingleProvider<>(begin);
    Codec<Simple, List<String>> toFlat = new AvroSpecificToStringList<>(source, Simple.class);

    StringWriter writer = new StringWriter();
    new CsvSink(writer).appendAll(toFlat);

    assertEquals("\"1\",\"te,s\"\"t\",\"-1.1\"", writer.toString().trim());
    assertNotNull(toFlat.getAllCounters());
  }

  @Test
  public void testCompositeSerialization() throws IOException {
    CharSequence[] names = new CharSequence[]{"red", "blue"};
    HashMap<CharSequence, Boolean> map = new HashMap<CharSequence, Boolean>();
    map.put("foo", true);
    Composite
        begin =
        Composite.newBuilder().setId(1).setNames(Arrays.asList(names)).setFlags(map).build();

    Provider<Composite> source = new SingleProvider<>(begin);
    Provider<byte[]>
        toBytes =
        new ValueToJsonBytes(new AvroSpecificToValue<>(source, Composite.class));

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    new BytesLineSink(baos).appendAll(toBytes);

    assertEquals("{\"id\":1,\"names\":[\"red\",\"blue\"],\"flags\":{\"foo\":true}}",
                 baos.toString("UTF-8").trim());
  }

  @Test
  public void testNestedSerialization() throws IOException {
    Simple simple = Simple.newBuilder().setFoo(2).setBar("sausage").setBaz(99.9).build();
    Map<CharSequence, List<Simple>> map = new HashMap<>();
    map.put("clef", Lists.newArrayList(simple));
    Nested begin = Nested.newBuilder().setNumbers(Arrays.asList(1.0f, -1.0f)).setUgly(map).build();

    Provider<Nested> source = new SingleProvider<>(begin);
    Provider<byte[]>
        toBytes =
        new ValueToJsonBytes(new AvroSpecificToValue<>(source, Nested.class));

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    new BytesLineSink(baos).appendAll(toBytes);

    assertEquals(
        "{\"type\":\"IE\",\"ugly\":{\"clef\":[{\"foo\":2,\"bar\":\"sausage\",\"baz\":99.9}]},\"numbers\":[1.0,-1.0]}",
        baos.toString("UTF-8").trim());
  }

  @Test
  public void testSimpleCsvGeneric() throws IOException {
    AvroProvider<List<String>> source = new CsvWithHeaderSource(readResource("/simple.csv"));
    Codec<List<String>, GenericRecord>
        fromFlat =
        new StringListToAvro<>(source, source.getAvroSchema());
    List<GenericRecord> list = Lists.newArrayList(fromFlat.iterator());
    assertEquals(2, list.size());
    GenericRecord r1 = list.get(0);
    GenericRecord r2 = list.get(1);

    assertEquals("1", r1.get("foo").toString());
    assertNull(r1.get("dummy"));
    assertEquals("alpha", r1.get("bar").toString());
    assertEquals("23.4", r1.get("baz").toString());

    assertEquals(" 2 ", r2.get("foo").toString());
    assertNull(r2.get("dummy"));
    assertEquals("bra\"f'in\"\nvo", r2.get("bar").toString());
    assertEquals(".56", r2.get("baz").toString());
  }

  @Test
  public void testSimpleCsvSpecific() throws IOException {
    AvroProvider<List<String>> source = new CsvWithHeaderSource(readResource("/simple.csv"));
    Codec<GenericRecord, Value> toTree =
        new AvroGenericToValue(new StringListToAvro<GenericRecord>(source, source.getAvroSchema()));
    Codec<Value, Simple> fromTree = new ValueToAvro<>(toTree, Simple.class);
    List<Simple> list = Lists.newArrayList(fromTree.iterator());

    assertEquals(2, list.size());
    Simple r1 = list.get(0);
    Simple r2 = list.get(1);

    assertEquals(1, r1.getFoo().longValue());
    assertEquals("alpha", r1.getBar().toString());
    assertEquals(23.4, r1.getBaz(), 0.0);

    assertEquals(2, r2.getFoo().longValue());
    assertEquals("bra\"f'in\"\nvo", r2.getBar().toString());
    assertEquals(.56, r2.getBaz(), 0.0);
  }

  @Test
  public void testSimpleTsv() throws IOException {
    Provider<List<String>> source = new TsvSource(readResource("/simple.tsv"));
    Codec<List<String>, Simple> fromFlat = new StringListToAvro<>(source, Simple.class);
    List<Simple> list = Lists.newArrayList(fromFlat.iterator());

    assertEquals(2, list.size());
    Simple r1 = list.get(0);
    Simple r2 = list.get(1);

    assertEquals(3, r1.getFoo().longValue());
    assertEquals("zig", r1.getBar().toString());
    assertEquals(0.0, r1.getBaz(), 0.0);

    assertEquals(4, r2.getFoo().longValue());
    assertEquals("zag", r2.getBar().toString());
    assertEquals(1.0, r2.getBaz(), 0.0);
  }

  @Test
  public void testSimpleJson() throws IOException {
    Provider<JsonNode> source = new JsonNodeSource(streamResource("/simple.json"));
    Codec<JsonNode, Simple> fromTree = new JsonNodeToAvro<>(source, Simple.class);
    List<Simple> list = Lists.newArrayList(fromTree.iterator());

    assertEquals(2, list.size());
    Simple r1 = list.get(0);
    Simple r2 = list.get(1);

    assertEquals(5, r1.getFoo().longValue());
    assertEquals("floobidoo", r1.getBar().toString());
    assertEquals(99.9, r1.getBaz(), 0.0);

    assertEquals(6, r2.getFoo().longValue());
    assertEquals("globglob", r2.getBar().toString());
    assertEquals(100.0, r2.getBaz(), 0.0);
  }

  @Test
  public void testCompositeJson() throws IOException {
    Provider<JsonNode> source = new JsonNodeSource(streamResource("/composite.json"));
    Codec<JsonNode, Composite> fromTree = new JsonNodeToAvro<>(source, Composite.class);
    List<Composite> list = Lists.newArrayList(fromTree.iterator());

    assertEquals(2, list.size());
    Composite r1 = list.get(0);
    Composite r2 = list.get(1);

    assertEquals(1, r1.getId().longValue());
    List<CharSequence> names = r1.getNames();
    assertNotNull(names);
    assertEquals(3, names.size());
    assertEquals("foo", names.get(0).toString());
    assertEquals("bar", names.get(1).toString());
    assertEquals("baz", names.get(2).toString());
    Map<CharSequence, Boolean> flags = r1.getFlags();
    assertNotNull(flags);
    assertEquals(3, flags.size());
    assertEquals(Boolean.TRUE, flags.get("foo"));
    assertEquals(Boolean.FALSE, flags.get("bar"));
    assertNull(flags.get("baz"));

    assertNull(r2.getId());
    assertNotNull(r2.getNames());
    assertEquals(0, r2.getNames().size());
    assertEquals(0, r2.getFlags().size());
  }


  @Test
  public void testNestedJson() throws IOException {
    Provider<JsonNode> source = new JsonNodeSource(streamResource("/nested.json"));
    Codec<JsonNode, Nested> fromTree = new JsonNodeToAvro<>(source, Nested.class);
    assertTrue(fromTree.hasNext());
    Nested r = fromTree.next();
    assertTrue(!fromTree.hasNext());

    assertNotNull(r.getType());
    assertEquals(BrowserType.IE, r.getType());
    assertNotNull(r.getUgly());
    assertTrue(r.getUgly().containsKey("clef"));
    assertNotNull(r.getUgly().get("clef"));
    assertEquals(1, r.getUgly().get("clef").size());

    Simple s = r.getUgly().get("clef").get(0);
    assertNotNull(s);
    assertNotNull(s.getFoo());
    assertEquals(5, s.getFoo().longValue());
    assertNotNull(s.getBar());
    assertEquals("floobidoo", s.getBar().toString());
    assertNotNull(s.getBaz());
    assertEquals(99.9, s.getBaz(), 0.0);

    List<Float> numbers = r.getNumbers();
    assertNotNull(numbers);
    assertEquals(4, numbers.size());
    assertNull(numbers.get(0));
    assertNotNull(numbers.get(1));
    assertEquals(12.3f, numbers.get(1).floatValue(), 0.0f);
    assertNull(numbers.get(2));
    assertNotNull(numbers.get(3));
    assertEquals(45.6f, numbers.get(3).floatValue(), 45.6f);
  }
}
