package com.adgear.anoa.read;

import com.adgear.anoa.AnoaJacksonTypeException;
import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;

class IntegerReader extends AbstractReader<Integer> {

  @Override
  protected Integer read(JsonParser jacksonParser) throws IOException {
    return jacksonParser.getValueAsInt();
  }

  @Override
  protected Integer readStrict(JsonParser jacksonParser) throws AnoaJacksonTypeException, IOException {
    switch (jacksonParser.getCurrentToken()) {
      case VALUE_NUMBER_INT:
        return jacksonParser.getIntValue();
      case VALUE_NULL:
        return null;
      default:
        throw new AnoaJacksonTypeException("Token is not integer: " + jacksonParser.getCurrentToken());
    }
  }
}
