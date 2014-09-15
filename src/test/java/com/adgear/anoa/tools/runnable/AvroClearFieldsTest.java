package com.adgear.anoa.tools.runnable;


import com.adgear.anoa.sink.CollectionSink;
import com.adgear.anoa.tools.codec.CleanserCodec;
import com.adgear.generated.avro.RecordNested;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;

public class AvroClearFieldsTest extends TestBase {

  @Test
  public void testSame() {

    ArrayList<RecordNested> expected = new CollectionSink<>(new ArrayList<RecordNested>())
        .appendAll(open("/multirecord_cleared.json"))
        .getCollection();

    ArrayList<RecordNested> actual = new CollectionSink<>(new ArrayList<RecordNested>())
        .appendAll(open("/multirecord_cleared.json"))
        .getCollection();

    Assert.assertArrayEquals(expected.toArray(), actual.toArray());
  }

  @Test
  public void testCleansed() {

    ArrayList<RecordNested> expected = new CollectionSink<>(new ArrayList<RecordNested>())
        .appendAll(open("/multirecord_cleared.json"))
        .getCollection();

    ArrayList<RecordNested> actual = new CollectionSink<>(new ArrayList<RecordNested>())
        .appendAll(new CleanserCodec<>(open(), new String[]{"count", "nested.id"}))
        .getCollection();

    Assert.assertArrayEquals(expected.toArray(), actual.toArray());
  }

  @Test
  public void testCleansedStreams() throws IOException {

    byte[] expected = toJson(open("/multirecord_cleared.json"));
    byte[] actual = toJson(new CleanserCodec<>(open(), new String[]{"count", "nested.id"}));

    Assert.assertArrayEquals(expected, actual);
  }


  @Test
  public void testTool() throws IOException {
    byte[] expected = toJson(open("/multirecord_cleared.json"));

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    new AvroClearFields<>(RecordNested.class,
                          new String[]{"count", "nested.id"},
                          toAvroStream(open()),
                          baos)
        .execute();
    byte[] actual = toJson(fromAvro(baos.toByteArray()));

    Assert.assertArrayEquals(expected, actual);
  }
}
