package com.adgear.anoa.read;

import com.adgear.anoa.AnoaTypeException;
import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;

class BooleanReader extends JacksonReader<Boolean> {

  @Override
  public Boolean read(JsonParser jp) throws IOException {
    switch (jp.getCurrentToken()) {
      case VALUE_TRUE:
        return true;
      case VALUE_FALSE:
        return false;
      default:
        gobbleValue(jp);
        return null;
    }
  }

  @Override
  public Boolean readStrict(JsonParser jp) throws AnoaTypeException, IOException {
    switch (jp.getCurrentToken()) {
      case VALUE_TRUE:
        return true;
      case VALUE_FALSE:
        return false;
      case VALUE_NULL:
        return null;
      default:
        throw new AnoaTypeException("Token is not boolean: " + jp.getCurrentToken());
    }
  }
}
