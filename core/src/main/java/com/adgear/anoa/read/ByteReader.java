package com.adgear.anoa.read;

import com.adgear.anoa.AnoaTypeException;
import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;

class ByteReader extends AbstractReader<Byte> {

  @Override
  protected Byte read(JsonParser jacksonParser) throws IOException {
    int intValue = jacksonParser.getValueAsInt();
    return (intValue > Byte.MAX_VALUE || intValue < Byte.MIN_VALUE) ? 0 : ((byte) intValue);
  }

  @Override
  protected Byte readStrict(JsonParser jacksonParser) throws AnoaTypeException, IOException {
    switch (jacksonParser.getCurrentToken()) {
      case VALUE_NUMBER_INT:
        int intValue = jacksonParser.getIntValue();
        if (intValue > Byte.MAX_VALUE || intValue < Byte.MIN_VALUE) {
          throw new AnoaTypeException(jacksonParser.getText() + " is an out of bounds integer for Byte.");
        }
        return (byte) intValue;
      case VALUE_NULL:
        return null;
      default:
        throw new AnoaTypeException("Token is not integer: " + jacksonParser.getCurrentToken());
    }
  }
}
