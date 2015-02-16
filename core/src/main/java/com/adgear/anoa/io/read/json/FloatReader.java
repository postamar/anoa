package com.adgear.anoa.io.read.json;

import com.adgear.anoa.AnoaTypeException;
import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;

class FloatReader extends JsonReader<Float> {

  @Override
  public Float read(JsonParser jp) throws IOException {
    switch (jp.getCurrentToken()) {
      case VALUE_NUMBER_FLOAT:
        return jp.getFloatValue();
      case VALUE_NUMBER_INT:
        try {
          return (float) jp.getValueAsDouble();
        } catch (NumberFormatException e) {
          return null;
        }
      default:
        gobbleValue(jp);
        return null;
    }
  }

  @Override
  public Float readStrict(JsonParser jp) throws AnoaTypeException, IOException {
    switch (jp.getCurrentToken()) {
      case VALUE_NUMBER_FLOAT:
        return jp.getFloatValue();
      case VALUE_NUMBER_INT:
        try {
          return (float) jp.getValueAsDouble();
        } catch (NumberFormatException e) {
          throw new AnoaTypeException(e);
        }
      case VALUE_NULL:
        return null;
      default:
        throw new AnoaTypeException("Token is not number: " + jp.getCurrentToken());
    }  }
}
