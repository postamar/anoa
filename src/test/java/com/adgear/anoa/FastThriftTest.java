package com.adgear.anoa;

import com.adgear.anoa.codec.schemaless.BytesToJsonNode;
import com.adgear.anoa.codec.thrift.JsonNodeToThrift;
import com.adgear.anoa.sink.ArrayListSink;
import com.adgear.anoa.source.serialized.BytesLineSource;
import com.adgear.generated.thrift.BrowserType;
import com.adgear.generated.thrift.Nested2;

import org.junit.Test;

import java.io.InputStream;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class FastThriftTest {

  @Test
  public void testNested2() throws Exception {

    InputStream stream = getClass().getResourceAsStream("/nested2.json");

    List<Nested2> list = new ArrayListSink<Nested2>()
        .appendAll(new JsonNodeToThrift<>(new BytesToJsonNode(new BytesLineSource(stream)),
                                              Nested2.class))
        .getCollection();

    assertEquals(1, list.size());

    Nested2 n = list.get(0);
    assertEquals(n.getType(), BrowserType.CHROME);
    assertEquals(n.getNumbersSize(), 2);
    assertEquals(n.getUglySize(), 1);
  }
}
