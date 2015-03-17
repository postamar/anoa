package com.adgear.anoa.read;

import com.adgear.anoa.AnoaTypeException;
import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;

class BooleanReader extends AbstractReader<Boolean> {

  @Override
  protected Boolean read(JsonParser jacksonParser) throws IOException {
    return jacksonParser.getValueAsBoolean();
  }

  @Override
  protected Boolean readStrict(JsonParser jacksonParser) throws AnoaTypeException, IOException {
    switch (jacksonParser.getCurrentToken()) {
      case VALUE_TRUE:
        return true;
      case VALUE_FALSE:
        return false;
      case VALUE_NULL:
        return null;
      default:
        throw new AnoaTypeException("Token is not boolean: " + jacksonParser.getCurrentToken());
    }
  }
}
