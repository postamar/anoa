package com.adgear.anoa.tools.runnable;

import com.adgear.anoa.sink.CollectionSink;
import com.adgear.anoa.tools.codec.FilterCodec;
import com.adgear.generated.avro.RecordNested;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;

public class AvroGrepTest extends TestBase {

  @Test
  public void testSame() {

    ArrayList<RecordNested> expected = new CollectionSink<>(new ArrayList<RecordNested>())
        .appendAll(open("/multirecord_filtered.json"))
        .getCollection();

    ArrayList<RecordNested> actual = new CollectionSink<>(new ArrayList<RecordNested>())
        .appendAll(open("/multirecord_filtered.json"))
        .getCollection();

    Assert.assertArrayEquals(expected.toArray(), actual.toArray());
  }

  @Test
  public void testFiltered() {

    ArrayList<RecordNested> expected = new CollectionSink<>(new ArrayList<RecordNested>())
        .appendAll(open("/multirecord_filtered.json"))
        .getCollection();

    ArrayList<RecordNested> actual = new CollectionSink<>(new ArrayList<RecordNested>())
        .appendAll(new FilterCodec<>(open(), RecordNested.class, "id IS NOT NULL AND count = 0"))
        .getCollection();

    Assert.assertArrayEquals(expected.toArray(), actual.toArray());
  }


  @Test
  public void testStream() throws IOException {
    byte[] expected = toJson(open("/multirecord_filtered.json"));
    byte[] actual = toJson(new FilterCodec<>(open(),
                                             RecordNested.class,
                                             "id IS NOT NULL AND count = 0"));

    Assert.assertArrayEquals(expected, actual);
  }

  @Test
  public void testTool() throws IOException {
    byte[] expected = toJson(open("/multirecord_filtered.json"));

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    new AvroGrep<>(RecordNested.class,
                   "id IS NOT NULL AND count = 0",
                   toAvroStream(open()),
                   baos)
        .execute();
    byte[] actual = toJson(fromAvro(baos.toByteArray()));

    Assert.assertArrayEquals(expected, actual);
  }


}
