package com.adgear.anoa.tools.codec;

import com.adgear.anoa.DeserializerTest;
import com.adgear.anoa.codec.avro.JsonNodeToAvro;
import com.adgear.anoa.provider.IteratorProvider;
import com.adgear.anoa.sink.CollectionSink;
import com.adgear.anoa.source.schemaless.JsonNodeSource;
import com.adgear.generated.avro.RecordNested;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FilterManyTest {

  private final int length = 13;
  private List<RecordNested> in;

  @Before
  public void setup() {
    in = new CollectionSink<>(new ArrayList<RecordNested>())
        .appendAll(
            new JsonNodeToAvro<>(
                new JsonNodeSource(DeserializerTest.streamResource("/multirecord.json")),
                RecordNested.class))
        .getCollection();
  }

  private List<RecordNested> applyFilter(String filter) {
    return new CollectionSink<>(new ArrayList<RecordNested>())
        .appendAll(new FilterCodec<>(new IteratorProvider<>(in.iterator()),
                                     RecordNested.class,
                                     filter))
        .getCollection();
  }

  @Test
  public void TrivialFull() throws IOException {
    Assert.assertEquals(length, applyFilter("TRUE").size());
  }

  @Test
  public void TrivialEmpty() throws IOException {
    Assert.assertEquals(0, applyFilter("FALSE").size());
  }

  @Test
  public void CountNullIds() throws IOException {
    Assert.assertEquals(1, applyFilter("id IS NULL").size());
  }

  @Test
  public void IdRange() throws IOException {
    Assert.assertEquals(3, applyFilter("id BETWEEN 2 AND 4").size());
  }

  @Test
  public void Disjunction() throws IOException {
    Assert.assertEquals(3, applyFilter("browser = 'FIREFOX' OR id IS NULL").size());
  }

  @Test
  public void CountNested() throws IOException {
    Assert.assertEquals(4, applyFilter("nested IS NOT NULL").size());
  }

  @Test
  public void Nested() throws IOException {
    Assert.assertEquals(2, applyFilter("nested IS NOT NULL AND nested.id > 16").size());
  }

  @Test
  public void NestedDisjunction() throws IOException {
    Assert.assertEquals(3, applyFilter("nested IS NOT NULL AND (nested.id > 16 OR browser = 'IE')")
        .size());
  }
}
