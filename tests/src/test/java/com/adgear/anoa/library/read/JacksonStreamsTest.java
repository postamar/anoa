package com.adgear.anoa.library.read;

import com.adgear.anoa.Anoa;
import com.adgear.anoa.AnoaHandler;
import com.adgear.anoa.read.JacksonStreams;
import com.adgear.anoa.test.AnoaTestSample;
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

  final static AnoaTestSample ATS = new AnoaTestSample();

  static public JacksonStreams<ObjectNode, ObjectMapper, JsonFactory, ?, JsonParser> build() {
    return new JacksonStreams<>(new ObjectMapper(), Optional.<FormatSchema>empty());
  }

  @Test
  public void testParser() throws IOException {
    build().parser(ATS.jsonInputStream(-1)).readValueAsTree();
  }

  @Test
  public void testObjects() {
    ATS.assertJsonObjects(build().from(ATS.jsonInputStream(-1)));
  }

  @Test
  public void testAnoaObjects() {
    List<Anoa<ObjectNode, Throwable>> list = build()
        .from(AnoaHandler.NO_OP_HANDLER, ATS.jsonInputStream(-1))
        .collect(Collectors.toList());
    Assert.assertEquals(ATS.n + 1, list.size());
    ATS.assertJsonObjects(list.stream().flatMap(Anoa::asStream));
  }
}
