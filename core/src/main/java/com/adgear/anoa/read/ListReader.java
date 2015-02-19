package com.adgear.anoa.read;

import com.adgear.anoa.AnoaTypeException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.IOException;
import java.util.ArrayList;

class ListReader extends JacksonReader<ArrayList<Object>> {

  final JacksonReader<?> elementReader;

  ListReader(JacksonReader<?> elementReader) {
    this.elementReader = elementReader;
  }

  @Override
  public ArrayList<Object> read(JsonParser jp) throws IOException {
    if (jp.getCurrentToken() == JsonToken.START_ARRAY) {
      ArrayList<Object> result = new ArrayList<>();
      doArray(jp, p -> result.add(elementReader.read(p)));
      return result;
    } else {
      gobbleValue(jp);
      return null;
    }
  }

  @Override
  public ArrayList<Object> readStrict(JsonParser jp) throws AnoaTypeException, IOException {
    switch (jp.getCurrentToken()) {
      case VALUE_NULL:
        return null;
      case START_ARRAY:
        ArrayList<Object> result = new ArrayList<>();
        doArray(jp, p -> result.add(elementReader.readStrict(p)));
        return result;
      default:
        throw new AnoaTypeException("Token is not '[': " + jp.getCurrentToken());
    }
  }
}
