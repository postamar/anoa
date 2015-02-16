package com.adgear.anoa.io.read.json;

import com.adgear.anoa.AnoaTypeException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.IOException;

class ShortReader extends JsonReader<Short> {

  @Override
  public Short read(JsonParser jp) throws IOException {
    if (jp.getCurrentToken() != JsonToken.VALUE_NUMBER_INT) {
      gobbleValue(jp);
      return null;
    }
    final int i = jp.getIntValue();
    return (i > Short.MAX_VALUE || i < Short.MIN_VALUE) ? null : ((short) i);
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
