package com.adgear.anoa.read;

import com.google.protobuf.Descriptors;

import com.adgear.anoa.AnoaJacksonTypeException;
import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

class ProtobufEnumReader extends AbstractReader<Descriptors.EnumValueDescriptor> {

  final private Descriptors.EnumDescriptor descriptor;
  final private Descriptors.EnumValueDescriptor defaultValue;
  final private Map<String, Descriptors.EnumValueDescriptor> labelLookUp;

  ProtobufEnumReader(Descriptors.EnumDescriptor descriptor,
                     Descriptors.EnumValueDescriptor defaultValue) {
    this.descriptor = descriptor;
    this.defaultValue = defaultValue;
    labelLookUp = new HashMap<>();
    for (Descriptors.EnumValueDescriptor value : descriptor.getValues()) {
      labelLookUp.put(value.getName(), value);
      labelLookUp.put(value.getName().toLowerCase(), value);
    }
  }

  @Override
  protected Descriptors.EnumValueDescriptor read(JsonParser jacksonParser) throws IOException {
    Descriptors.EnumValueDescriptor value;
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
        break;
      case VALUE_NUMBER_INT:
        value = descriptor.findValueByNumber(jacksonParser.getIntValue());
        break;
      default:
        gobbleValue(jacksonParser);
        return null;
    }
    return (value == null) ? defaultValue : value;
  }

  @Override
  protected Descriptors.EnumValueDescriptor readStrict(JsonParser jacksonParser)
      throws AnoaJacksonTypeException, IOException {
    Descriptors.EnumValueDescriptor value;
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
          throw new AnoaJacksonTypeException(
              "Invalid label " + label + " for " + descriptor);
        }
        return value;
      case VALUE_NUMBER_INT:
        value = descriptor.findValueByNumber(jacksonParser.getIntValue());
        if (value == null) {
          throw new AnoaJacksonTypeException(
              "Invalid ordinal " + jacksonParser.getText() + " for " + descriptor);
        }
        return value;
      case VALUE_NULL:
        return defaultValue;
      default:
        throw new AnoaJacksonTypeException(
            "Token is not enum label or ordinal: " + jacksonParser.getCurrentToken());
    }
  }
}
