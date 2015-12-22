package com.adgear.anoa.read;

import com.adgear.anoa.Anoa;
import com.adgear.anoa.AnoaHandler;
import com.adgear.anoa.BidReqs;
import com.fasterxml.jackson.core.FormatSchema;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import junitx.framework.Assert;

public class JacksonStreamsTest {

  static public JacksonStreams<ObjectNode, ObjectMapper, JsonFactory, ?, JsonParser> build() {
    return new JacksonStreams<>(new ObjectMapper(), Optional.<FormatSchema>empty());
  }

  @Test
  public void testParser() throws IOException {
    build().parser(BidReqs.jsonBytes(-1)).readValueAsTree();
  }

  @Test
  public void testObjects() {
    BidReqs.assertJsonObjects(build().from(BidReqs.jsonBytes(-1)));
  }

  @Test
  public void testAnoaObjects() {
    List<Anoa<ObjectNode, Throwable>> list = build()
        .from(AnoaHandler.NO_OP_HANDLER, BidReqs.jsonBytes(-1))
        .collect(Collectors.toList());
    Assert.assertEquals(BidReqs.n + 1, list.size());
    BidReqs.assertJsonObjects(list.stream().flatMap(Anoa::asStream));
  }
}
