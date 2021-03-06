package com.adgear.anoa.read;

import com.adgear.anoa.AnoaJacksonTypeException;
import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;

class DoubleReader extends AbstractReader<Double> {

  @Override
  protected Double read(JsonParser jacksonParser) throws IOException {
    return jacksonParser.getValueAsDouble();
  }

  @Override
  protected Double readStrict(JsonParser jacksonParser)
      throws AnoaJacksonTypeException, IOException {
    switch (jacksonParser.getCurrentToken()) {
      case VALUE_NUMBER_FLOAT:
        return jacksonParser.getDoubleValue();
      case VALUE_NUMBER_INT:
        try {
          return jacksonParser.getValueAsDouble();
        } catch (NumberFormatException e) {
          throw new AnoaJacksonTypeException(e);
        }
      case VALUE_NULL:
        return null;
      default:
        throw new AnoaJacksonTypeException(
            "Token is not number: " + jacksonParser.getCurrentToken());
    }
  }
}
