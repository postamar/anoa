package com.adgear.anoa.io.read.json;

import com.adgear.anoa.AnoaTypeException;
import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;

class DoubleReader extends JsonReader<Double> {

  @Override
  public Double read(JsonParser jp) throws IOException {
    switch (jp.getCurrentToken()) {
      case VALUE_NUMBER_FLOAT:
        return jp.getDoubleValue();
      case VALUE_NUMBER_INT:
        return jp.getValueAsDouble();
      default:
        gobbleValue(jp);
        return null;
    }
  }

  @Override
  public Double readStrict(JsonParser jp) throws AnoaTypeException, IOException {
    switch (jp.getCurrentToken()) {
      case VALUE_NUMBER_FLOAT:
        return jp.getDoubleValue();
      case VALUE_NUMBER_INT:
        return jp.getValueAsDouble();
      case VALUE_NULL:
        return null;
      default:
        throw new AnoaTypeException("Token is not number: " + jp.getCurrentToken());
    }
  }
}
