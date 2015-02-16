package com.adgear.anoa.io.read.json;

import com.adgear.anoa.AnoaTypeException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.IOException;

class ByteReader extends JsonReader<Byte> {

  @Override
  public Byte read(JsonParser jp) throws IOException {
    if (jp.getCurrentToken() != JsonToken.VALUE_NUMBER_INT) {
      gobbleValue(jp);
      return null;
    }
    int intValue = jp.getIntValue();
    return (intValue > Byte.MAX_VALUE || intValue < Byte.MIN_VALUE) ? null : ((byte) intValue);
  }

  @Override
  public Byte readStrict(JsonParser jp) throws AnoaTypeException, IOException {
    switch (jp.getCurrentToken()) {
      case VALUE_NUMBER_INT:
        int intValue = jp.getIntValue();
        if (intValue > Byte.MAX_VALUE || intValue < Byte.MIN_VALUE) {
          throw new AnoaTypeException(jp.getText() + " is an out of bounds integer for Byte.");
        }
        return (byte) intValue;
      case VALUE_NULL:
        return null;
      default:
        throw new AnoaTypeException("Token is not integer: " + jp.getCurrentToken());
    }
  }
}
