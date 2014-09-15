package com.adgear.anoa.source.schemaless;

import com.adgear.anoa.provider.base.CounterlessProviderBase;
import com.adgear.anoa.source.Source;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;

/**
 * Iterates over JSON objects, exposed as Jackson JsonNode instances.
 */
public class JsonNodeSource extends CounterlessProviderBase<JsonNode> implements Source<JsonNode> {

  protected ObjectMapper objectMapper;
  protected JsonParser jsonParser;
  protected JsonNode nextValue = null;

  public JsonNodeSource(InputStream in) {
    this.objectMapper = new ObjectMapper();
    try {
      this.jsonParser = objectMapper.getFactory().createJsonParser(in);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws IOException {
    jsonParser.close();
  }

  protected JsonNode peek() throws IOException {
    if (nextValue != null) {
      return nextValue;
    }
    nextValue = objectMapper.readTree(jsonParser);
    return nextValue;
  }

  @Override
  protected JsonNode getNext() throws IOException {
    JsonNode rValue = peek();
    nextValue = null;
    return rValue;
  }

  @Override
  public boolean hasNext() {
    try {
      return (peek() != null);
    } catch (IOException e) {
      logger.error(e.getMessage());
      return false;
    }
  }
}
