package com.adgear.anoa.io.read.json;

import com.adgear.anoa.AnoaTypeException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.IOException;

class LongReader extends JsonReader<Long> {

  @Override
  public Long read(JsonParser jp) throws IOException {
    if (jp.getCurrentToken() == JsonToken.VALUE_NUMBER_INT) {
      return jp.getLongValue();
    } else {
      gobbleValue(jp);
      return null;
    }
  }

  @Override
  public Long readStrict(JsonParser jp) throws AnoaTypeException, IOException {
    switch (jp.getCurrentToken()) {
      case VALUE_NUMBER_INT:
        return jp.getLongValue();
      case VALUE_NULL:
        return null;
      default:
        throw new AnoaTypeException("Token is not integer: " + jp.getCurrentToken());
    }
  }
}
