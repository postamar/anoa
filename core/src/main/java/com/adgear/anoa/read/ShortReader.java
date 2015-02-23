package com.adgear.anoa.read;

import com.adgear.anoa.AnoaTypeException;
import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;

class ShortReader extends JacksonReader<Short> {

  @Override
  public Short read(JsonParser jp) throws IOException {
    final int i = jp.getValueAsInt();
    return (i > Short.MAX_VALUE || i < Short.MIN_VALUE) ? 0 : ((short) i);
  }

  @Override
  public Short readStrict(JsonParser jp) throws AnoaTypeException, IOException {
    switch (jp.getCurrentToken()) {
      case VALUE_NUMBER_INT:
        final int intValue = jp.getIntValue();
        if (intValue > Short.MAX_VALUE || intValue < Short.MIN_VALUE) {
          throw new AnoaTypeException(jp.getText() + " is out of bounds for I16.");
        }
        return (short) intValue;
      case VALUE_NULL:
        return null;
      default:
        throw new AnoaTypeException("Token is not integer: " + jp.getCurrentToken());
    }
  }
}
