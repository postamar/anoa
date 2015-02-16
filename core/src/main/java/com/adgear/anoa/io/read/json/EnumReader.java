package com.adgear.anoa.io.read.json;

import com.adgear.anoa.AnoaTypeException;
import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

class EnumReader extends JsonReader<Enum> {

  final private Map<String,Enum> labelLookUp;
  final private Map<Integer,Enum> ordinalLookUp;
  final Class enumClass;

  @SuppressWarnings("unchecked")
  public EnumReader(Class enumClass) {
    this.enumClass = enumClass;
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
  public Enum read(JsonParser jp) throws IOException {
    switch (jp.getCurrentToken()) {
      case VALUE_STRING:
        final String label = jp.getText();
        Enum value = labelLookUp.get(label);
        if (value == null) {
          value = labelLookUp.get(label.toUpperCase());
          if (value != null) {
            labelLookUp.put(label, value);
          }
        }
        return value;
      case VALUE_NUMBER_INT:
        return ordinalLookUp.get(jp.getIntValue());
      default:
        gobbleValue(jp);
        return null;
    }
  }

  @Override
  public Enum readStrict(JsonParser jp) throws AnoaTypeException, IOException {
    Enum value;
    switch (jp.getCurrentToken()) {
      case VALUE_STRING:
        final String label = jp.getText();
        value = labelLookUp.get(label);
        if (value == null) {
          value = labelLookUp.get(label.toUpperCase());
          if (value != null) {
            labelLookUp.put(label, value);
          }
        }
        if (value == null) {
          throw new AnoaTypeException("Invalid label " + jp.getText() + " for " + enumClass);
        }
        return value;
      case VALUE_NUMBER_INT:
        value = ordinalLookUp.get(jp.getIntValue());
        if (value == null) {
          throw new AnoaTypeException("Invalid ordinal " + jp.getText() + " for " + enumClass);
        }
        return value;
      case VALUE_NULL:
        return null;
      default:
        throw new AnoaTypeException("Token is not enum label or ordinal: " + jp.getCurrentToken());
    }
  }
}
