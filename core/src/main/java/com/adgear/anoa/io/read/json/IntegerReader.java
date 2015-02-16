package com.adgear.anoa.io.read.json;

import com.adgear.anoa.AnoaTypeException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.IOException;

class IntegerReader extends JsonReader<Integer> {

  @Override
  public Integer read(JsonParser jp) throws IOException {
    if (jp.getCurrentToken() == JsonToken.VALUE_NUMBER_INT) {
      return jp.getIntValue();
    } else {
      gobbleValue(jp);
      return null;
    }
  }

  @Override
  public Integer readStrict(JsonParser jp) throws AnoaTypeException, IOException {
    switch (jp.getCurrentToken()) {
      case VALUE_NUMBER_INT:
        return jp.getIntValue();
      case VALUE_NULL:
        return null;
      default:
        throw new AnoaTypeException("Token is not integer: " + jp.getCurrentToken());
    }
  }
}
