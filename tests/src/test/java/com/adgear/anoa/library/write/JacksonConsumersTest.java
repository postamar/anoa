package com.adgear.anoa.library.write;

import com.adgear.anoa.read.JacksonStreamsTest;
import com.adgear.anoa.test.AnoaTestSample;
import com.fasterxml.jackson.core.FormatSchema;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.util.TokenBuffer;

import org.junit.Test;

import java.io.IOException;
import java.util.Optional;

public class JacksonConsumersTest {

  final static AnoaTestSample ATS = new AnoaTestSample();

  static public JacksonConsumers<ObjectNode, ObjectMapper, JsonFactory, ?, JsonGenerator> build() {
    return new JacksonConsumers<>(new ObjectMapper(), Optional.<FormatSchema>empty());
  }

  @Test
  public void testBuffer() throws IOException {
    try (TokenBuffer b = new TokenBuffer(AnoaTestSample.OBJECT_MAPPER, false)) {
      try (WriteConsumer<ObjectNode> wc = build().to(b)) {
        ATS.jsonObjects().forEach(wc);
      }
      ATS.assertJsonObjects(JacksonStreamsTest.build().from(b.asParser()));
    }
  }

  @Test
  public void testStream() throws IOException {
    ATS.assertJsonObjects(JacksonStreamsTest.build().from(ATS.allAsInputStream(
        os -> {
          try (WriteConsumer<ObjectNode> wc = build().to(os)) {
            ATS.jsonObjects().forEach(wc);
          }
        })));
  }
}
