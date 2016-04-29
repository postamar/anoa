package com.adgear.anoa.read;

import com.adgear.anoa.AnoaJacksonTypeException;
import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;

class LongReader extends AbstractReader<Long> {

  @Override
  protected Long read(JsonParser jacksonParser) throws IOException {
    return jacksonParser.getValueAsLong();
  }

  @Override
  protected Long readStrict(JsonParser jacksonParser) throws AnoaJacksonTypeException, IOException {
    switch (jacksonParser.getCurrentToken()) {
      case VALUE_NUMBER_INT:
        return jacksonParser.getLongValue();
      case VALUE_NULL:
        return null;
      default:
        throw new AnoaJacksonTypeException(
            "Token is not integer: " + jacksonParser.getCurrentToken());
    }
  }
}
