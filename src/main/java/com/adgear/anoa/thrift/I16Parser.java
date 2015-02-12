package com.adgear.anoa.thrift;

import com.fasterxml.jackson.databind.JsonNode;

import org.apache.thrift.TException;


class I16Parser extends JsonNodeParser<Short> {

  @Override
  Short parse(JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }
    if (!node.isInt()) {
      return null;
    }
    int intValue = node.asInt();
    if (intValue > Short.MAX_VALUE || intValue < Short.MIN_VALUE) {
      return null;
    }
    return (short) intValue;
  }

  @Override
  Short parseStrict(JsonNode node) throws TException {
    if (node == null || node.isNull()) {
      return null;
    }
    if (!node.isInt()) {
      throw new TException(node.asText() + " is not an int.");

    }
    int intValue = node.asInt();
    if (intValue > Short.MAX_VALUE || intValue < Short.MIN_VALUE) {
      throw new TException(node.asText() + " is out of bounds for I16.");
    }
    return (short) intValue;
  }
}
