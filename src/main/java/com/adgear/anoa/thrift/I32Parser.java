package com.adgear.anoa.thrift;

import com.fasterxml.jackson.databind.JsonNode;

import org.apache.thrift.TException;


class I32Parser extends JsonNodeParser<Integer> {

  @Override
  Integer parse(JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }
    if (!node.isInt()) {
      return null;
    }
    return node.asInt();
  }

  @Override
  Integer parseStrict(JsonNode node) throws TException {
    if (node == null || node.isNull()) {
      return null;
    }
    if (!node.isInt()) {
      throw new TException(node.asText() + " is not an int.");
    }
    return node.asInt();
  }
}
