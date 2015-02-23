package com.adgear.anoa.read;

import com.adgear.anoa.AnoaTypeException;
import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;

class ByteReader extends JacksonReader<Byte> {

  @Override
  public Byte read(JsonParser jp) throws IOException {
    int intValue = jp.getValueAsInt();
    return (intValue > Byte.MAX_VALUE || intValue < Byte.MIN_VALUE) ? 0 : ((byte) intValue);
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
