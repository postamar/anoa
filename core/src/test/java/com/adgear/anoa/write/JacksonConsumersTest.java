package com.adgear.anoa.write;


import com.adgear.anoa.BidReqs;
import com.adgear.anoa.read.JacksonStreamsTest;
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

  static public JacksonConsumers<ObjectNode, ObjectMapper, JsonFactory, ?, JsonGenerator> build() {
    return new JacksonConsumers<>(new ObjectMapper(), Optional.<FormatSchema>empty());
  }

  @Test
  public void testBuffer() throws IOException {
    try (TokenBuffer b = new TokenBuffer(BidReqs.objectMapper, false)) {
      try (WriteConsumer<ObjectNode> wc = build().to(b)) {
        BidReqs.jsonObjects().forEach(wc);
      }
      BidReqs.assertJsonObjects(JacksonStreamsTest.build().from(b.asParser()));
    }
  }

  @Test
  public void testStream() throws IOException {
    BidReqs.assertJsonObjects(JacksonStreamsTest.build().from(BidReqs.allAsStream(
        os -> {
          try (WriteConsumer<ObjectNode> wc = build().to(os)) {
            BidReqs.jsonObjects().forEach(wc);
          }
        })));
  }
}
