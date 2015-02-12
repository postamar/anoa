package com.adgear.anoa.thrift;

import com.fasterxml.jackson.databind.JsonNode;

import org.apache.thrift.TException;


class StringParser extends JsonNodeParser<String> {

  @Override
  String parse(JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }
    return node.asText();
  }

  @Override
  String parseStrict(JsonNode node) throws TException {
    if (node == null || node.isNull()) {
      return null;
    }
    if (!node.isTextual()) {
      throw new TException(node.asText() + " is not textual.");
    }
    return node.asText();
  }
}
