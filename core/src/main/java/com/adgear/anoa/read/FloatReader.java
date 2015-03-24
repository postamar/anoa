package com.adgear.anoa.read;

import com.adgear.anoa.AnoaJacksonTypeException;
import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;

class FloatReader extends AbstractReader<Float> {

  @Override
  protected Float read(JsonParser jacksonParser) throws IOException {
    return (float) jacksonParser.getValueAsDouble();
  }

  @Override
  protected Float readStrict(JsonParser jacksonParser) throws AnoaJacksonTypeException, IOException {
    switch (jacksonParser.getCurrentToken()) {
      case VALUE_NUMBER_FLOAT:
        return jacksonParser.getFloatValue();
      case VALUE_NUMBER_INT:
        try {
          return (float) jacksonParser.getValueAsDouble();
        } catch (NumberFormatException e) {
          throw new AnoaJacksonTypeException(e);
        }
      case VALUE_NULL:
        return null;
      default:
        throw new AnoaJacksonTypeException("Token is not number: " + jacksonParser.getCurrentToken());
    }  }
}
