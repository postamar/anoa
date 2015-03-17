package com.adgear.anoa.read;

import com.adgear.anoa.AnoaTypeException;
import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

class EnumReader extends AbstractReader<Enum> {

  final private Map<String,Enum> labelLookUp;
  final private Map<Integer,Enum> ordinalLookUp;
  final Class enumClass;

  @SuppressWarnings("unchecked")
  EnumReader(Class enumClass) {
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
  protected Enum read(JsonParser jacksonParser) throws IOException {
    switch (jacksonParser.getCurrentToken()) {
      case VALUE_STRING:
        final String label = jacksonParser.getText();
        Enum value = labelLookUp.get(label);
        if (value == null) {
          value = labelLookUp.get(label.toUpperCase());
          if (value != null) {
            labelLookUp.put(label, value);
          }
        }
        return value;
      case VALUE_NUMBER_INT:
        return ordinalLookUp.get(jacksonParser.getIntValue());
      default:
        gobbleValue(jacksonParser);
        return null;
    }
  }

  @Override
  protected Enum readStrict(JsonParser jacksonParser) throws AnoaTypeException, IOException {
    Enum value;
    switch (jacksonParser.getCurrentToken()) {
      case VALUE_STRING:
        final String label = jacksonParser.getText();
        value = labelLookUp.get(label);
        if (value == null) {
          value = labelLookUp.get(label.toUpperCase());
          if (value != null) {
            labelLookUp.put(label, value);
          }
        }
        if (value == null) {
          throw new AnoaTypeException("Invalid label " + jacksonParser.getText() + " for " + enumClass);
        }
        return value;
      case VALUE_NUMBER_INT:
        value = ordinalLookUp.get(jacksonParser.getIntValue());
        if (value == null) {
          throw new AnoaTypeException("Invalid ordinal " + jacksonParser.getText() + " for " + enumClass);
        }
        return value;
      case VALUE_NULL:
        return null;
      default:
        throw new AnoaTypeException("Token is not enum label or ordinal: " + jacksonParser.getCurrentToken());
    }
  }
}
