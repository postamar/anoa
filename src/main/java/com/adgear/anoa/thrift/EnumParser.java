package com.adgear.anoa.thrift;

import com.fasterxml.jackson.databind.JsonNode;

import org.apache.thrift.TException;
import org.apache.thrift.meta_data.EnumMetaData;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

class EnumParser extends JsonNodeParser<Enum> {

  final private Map<String,Enum> labelLookUp;
  final private Map<Integer,Enum> ordinalLookUp;
  final Class enumClass;

  @SuppressWarnings("unchecked")
  EnumParser(EnumMetaData enumMetaData) {
    enumClass = enumMetaData.enumClass;
    labelLookUp = new HashMap<>();
    ordinalLookUp = new HashMap<>();
    try {
      Enum[] values = (Enum[]) enumClass.getMethod("values").invoke(null);
      for (Enum value : values) {
        ordinalLookUp.put(value.ordinal(), value);
        labelLookUp.put(value.name(), value);
        labelLookUp.put(value.name().toLowerCase(), value);
      }
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  Enum parse(JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }
    if (node.isInt()) {
      return ordinalLookUp.get(node.asInt());
    } else {
      final String label = node.asText();
      Enum result = labelLookUp.get(label);
      if (result == null) {
        result = labelLookUp.get(label.toUpperCase());
        if (result != null) {
          labelLookUp.put(label, result);
        }
      }
      return result;
    }
  }

  @Override
  Enum parseStrict(JsonNode node) throws TException {
    if (node == null || node.isNull()) {
      return null;
    }
    Enum result;
    if (node.isInt()) {
      result = ordinalLookUp.get(node.asInt());
    } else {
      final String label = node.asText();
      result = labelLookUp.get(label);
      if (result == null) {
        result = labelLookUp.get(label.toUpperCase());
        if (result != null) {
          labelLookUp.put(label, result);
        }
      }
    }
    if (result == null) {
      throw new TException("Invalid ordinal " + node.asText() + " for enum class " + enumClass);
    }
    return result;
  }
}
