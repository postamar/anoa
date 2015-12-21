package com.adgear.anoa.read;

import com.adgear.anoa.AnoaJacksonTypeException;
import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;

class StringReader extends AbstractReader<String> {

  @Override
  protected String read(JsonParser jacksonParser) throws IOException {
    switch (jacksonParser.getCurrentToken()) {
      case VALUE_STRING:
        return jacksonParser.getText();
      case VALUE_NUMBER_FLOAT:
      case VALUE_NUMBER_INT:
      case VALUE_FALSE:
      case VALUE_TRUE:
      case VALUE_EMBEDDED_OBJECT:
        return jacksonParser.getValueAsString();
      default:
        gobbleValue(jacksonParser);
        return null;
    }
  }

  @Override
  protected String readStrict(JsonParser jacksonParser)
      throws AnoaJacksonTypeException, IOException {
    switch (jacksonParser.getCurrentToken()) {
      case VALUE_STRING:
        return jacksonParser.getText();
      case VALUE_NULL:
        return null;
      default:
        throw new AnoaJacksonTypeException(
            "Token is not string: " + jacksonParser.getCurrentToken());
    }
  }
}
