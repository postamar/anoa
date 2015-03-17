package com.adgear.anoa.read;

import com.adgear.anoa.AnoaTypeException;
import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;

class LongReader extends AbstractReader<Long> {

  @Override
  protected Long read(JsonParser jacksonParser) throws IOException {
    return jacksonParser.getValueAsLong();
  }

  @Override
  protected Long readStrict(JsonParser jacksonParser) throws AnoaTypeException, IOException {
    switch (jacksonParser.getCurrentToken()) {
      case VALUE_NUMBER_INT:
        return jacksonParser.getLongValue();
      case VALUE_NULL:
        return null;
      default:
        throw new AnoaTypeException("Token is not integer: " + jacksonParser.getCurrentToken());
    }
  }
}
