package com.adgear.anoa.read;

import com.adgear.anoa.AnoaJacksonTypeException;
import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;

class BooleanReader extends AbstractReader<Boolean> {

  @Override
  protected Boolean read(JsonParser jacksonParser) throws IOException {
    return jacksonParser.getValueAsBoolean();
  }

  @Override
  protected Boolean readStrict(JsonParser jacksonParser) throws AnoaJacksonTypeException, IOException {
    switch (jacksonParser.getCurrentToken()) {
      case VALUE_TRUE:
        return true;
      case VALUE_FALSE:
        return false;
      case VALUE_NULL:
        return null;
      default:
        throw new AnoaJacksonTypeException("Token is not boolean: " + jacksonParser.getCurrentToken());
    }
  }
}
