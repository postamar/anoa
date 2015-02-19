package com.adgear.anoa.read;

import com.adgear.anoa.AnoaTypeException;
import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;

class StringReader extends JacksonReader<String> {

  @Override
  public String read(JsonParser jp) throws IOException {
    switch (jp.getCurrentToken()) {
      case VALUE_STRING:
        return jp.getText();
      case VALUE_NUMBER_FLOAT:
      case VALUE_NUMBER_INT:
      case VALUE_FALSE:
      case VALUE_TRUE:
      case VALUE_EMBEDDED_OBJECT:
        return jp.getValueAsString();
      default:
        gobbleValue(jp);
        return null;
    }
  }

  @Override
  public String readStrict(JsonParser jp) throws AnoaTypeException, IOException {
    switch (jp.getCurrentToken()) {
      case VALUE_STRING:
        return jp.getText();
      case VALUE_NULL:
        return null;
      default:
        throw new AnoaTypeException("Token is not string: " + jp.getCurrentToken());
    }
  }
}
