package com.adgear.anoa.thrift;

import com.fasterxml.jackson.databind.JsonNode;

import org.apache.thrift.TException;

import java.io.IOException;
import java.nio.ByteBuffer;

class BinaryParser extends JsonNodeParser<ByteBuffer> {

  @Override
  ByteBuffer parse(JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }
    if (!node.isTextual()) {
      return null;
    }
    try {
      return ByteBuffer.wrap(node.binaryValue());
    } catch (IOException e) {
      return null;
    }
  }

  @Override
  ByteBuffer parseStrict(JsonNode node) throws TException {
    if (node == null || node.isNull()) {
      return null;
    }
    if (!node.isTextual()) {
      throw new TException(node.asText() + " is not textual.");
    }
    try {
      return ByteBuffer.wrap(node.binaryValue());
    } catch (IOException e) {
      throw new TException(e);
    }
  }
}
