package com.adgear.anoa.read;

import com.adgear.anoa.AnoaJacksonTypeException;
import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;

class ShortReader extends AbstractReader<Short> {

  @Override
  protected Short read(JsonParser jacksonParser) throws IOException {
    final int i = jacksonParser.getValueAsInt();
    return (i > Short.MAX_VALUE || i < Short.MIN_VALUE) ? 0 : ((short) i);
  }

  @Override
  protected Short readStrict(JsonParser jacksonParser)
      throws AnoaJacksonTypeException, IOException {
    switch (jacksonParser.getCurrentToken()) {
      case VALUE_NUMBER_INT:
        final int intValue = jacksonParser.getIntValue();
        if (intValue > Short.MAX_VALUE || intValue < Short.MIN_VALUE) {
          throw new AnoaJacksonTypeException(
              jacksonParser.getText() + " is out of bounds for I16.");
        }
        return (short) intValue;
      case VALUE_NULL:
        return null;
      default:
        throw new AnoaJacksonTypeException(
            "Token is not integer: " + jacksonParser.getCurrentToken());
    }
  }
}
