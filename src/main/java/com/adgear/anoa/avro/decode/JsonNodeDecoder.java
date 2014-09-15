package com.adgear.anoa.avro.decode;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.TextNode;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * {@link org.apache.avro.io.Decoder} implementation for deserializing Avro records from Jackson
 * <code>JsonNode</code> instances.
 */
public class JsonNodeDecoder extends TreeDecoderBase<JsonNode> {

  public JsonNodeDecoder(JsonNode input) {
    super(input);
  }

  @Override
  protected boolean isArray(JsonNode node) {
    return node.isArray();
  }

  @Override
  protected boolean isMap(JsonNode node) {
    return node.isObject();
  }

  @Override
  protected Iterator<JsonNode> getMapIterator(JsonNode node) {
    List<JsonNode> list = new ArrayList<>(node.size() * 2);
    Iterator<Map.Entry<String, JsonNode>> fieldIterator = node.fields();
    while (fieldIterator.hasNext()) {
      Map.Entry<String, JsonNode> field = fieldIterator.next();
      list.add(new TextNode(field.getKey()));
      list.add(field.getValue());
    }
    return list.iterator();
  }

  @Override
  protected Iterator<JsonNode> getArrayIterator(JsonNode node) {
    return node.elements();
  }

  @Override
  public boolean readBoolean() throws IOException {
    return parseBoolean(pop().asText());
  }

  @Override
  public int readInt() throws IOException {
    return parseInteger(pop().asText());
  }

  @Override
  public long readLong() throws IOException {
    return parseLong(pop().asText());
  }

  @Override
  public float readFloat() throws IOException {
    return parseFloat(pop().asText());
  }

  @Override
  public double readDouble() throws IOException {
    return parseDouble(pop().asText());
  }

  @Override
  public String readString() throws IOException {
    return pop().asText();
  }

  @Override
  public ByteBuffer readBytes(ByteBuffer old) throws IOException {
    return ByteBuffer.wrap(pop().binaryValue());
  }

  @Override
  public void readFixed(byte[] bytes, int start, int length) throws IOException {
    ByteBuffer bb = ByteBuffer.wrap(bytes, start, length);
    bb.put(pop().binaryValue());
  }

  @Override
  public long readArrayStart() throws IOException {
    return pop().size();
  }

  @Override
  public long readMapStart() throws IOException {
    return pop().size();
  }

  @Override
  public int readIndex() throws IOException {
    return peek().isNull() ? 0 : 1;
  }
}
