package com.adgear.anoa.read;

import com.adgear.anoa.AnoaTypeException;
import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;

class FloatReader extends AbstractReader<Float> {

  @Override
  protected Float read(JsonParser jacksonParser) throws IOException {
    return (float) jacksonParser.getValueAsDouble();
  }

  @Override
  protected Float readStrict(JsonParser jacksonParser) throws AnoaTypeException, IOException {
    switch (jacksonParser.getCurrentToken()) {
      case VALUE_NUMBER_FLOAT:
        return jacksonParser.getFloatValue();
      case VALUE_NUMBER_INT:
        try {
          return (float) jacksonParser.getValueAsDouble();
        } catch (NumberFormatException e) {
          throw new AnoaTypeException(e);
        }
      case VALUE_NULL:
        return null;
      default:
        throw new AnoaTypeException("Token is not number: " + jacksonParser.getCurrentToken());
    }  }
}
