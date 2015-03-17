package com.adgear.anoa.read;

import com.adgear.anoa.AnoaTypeException;
import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;

class IntegerReader extends AbstractReader<Integer> {

  @Override
  protected Integer read(JsonParser jacksonParser) throws IOException {
    return jacksonParser.getValueAsInt();
  }

  @Override
  protected Integer readStrict(JsonParser jacksonParser) throws AnoaTypeException, IOException {
    switch (jacksonParser.getCurrentToken()) {
      case VALUE_NUMBER_INT:
        return jacksonParser.getIntValue();
      case VALUE_NULL:
        return null;
      default:
        throw new AnoaTypeException("Token is not integer: " + jacksonParser.getCurrentToken());
    }
  }
}
