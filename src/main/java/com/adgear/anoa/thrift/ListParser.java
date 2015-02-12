package com.adgear.anoa.thrift;

import com.fasterxml.jackson.databind.JsonNode;

import org.apache.thrift.TException;
import org.apache.thrift.meta_data.ListMetaData;

import java.util.ArrayList;
import java.util.Iterator;

class ListParser extends JsonNodeParser<ArrayList<Object>> {

  final JsonNodeParser elementParser;

  ListParser(ListMetaData metaData) {
    this.elementParser = create(metaData.elemMetaData);
  }

  @Override
  ArrayList<Object> parse(JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }
    if (!node.isArray()) {
      return null;
    }
    ArrayList<Object> result = new ArrayList<>();
    Iterator<JsonNode> iterator = node.elements();
    while (iterator.hasNext()) {
      result.add(elementParser.parse(iterator.next()));
    }
    return result;
  }

  @Override
  ArrayList<Object> parseStrict(JsonNode node) throws TException {
    if (node == null || node.isNull()) {
      return null;
    }
    if (!node.isArray()) {
      throw new TException(node.asText() + " is not an array node.");
    }
    ArrayList<Object> result = new ArrayList<>();
    Iterator<JsonNode> iterator = node.elements();
    while (iterator.hasNext()) {
      result.add(elementParser.parseStrict(iterator.next()));
    }
    return result;
  }
}
