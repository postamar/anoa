package com.adgear.anoa.read;

import com.adgear.anoa.AnoaTypeException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.IOException;
import java.util.HashSet;

class SetReader extends JacksonReader<HashSet<Object>> {

  final JacksonReader<?> elementReader;

  public SetReader(JacksonReader<?> elementReader) {
    this.elementReader = elementReader;
  }

  @Override
  public HashSet<Object> read(JsonParser jp) throws IOException {
    if (jp.getCurrentToken() == JsonToken.START_ARRAY) {
      HashSet<Object> result = new HashSet<>();
      doArray(jp, p -> result.add(elementReader.read(p)));
      return result;
    } else {
      gobbleValue(jp);
      return null;
    }
  }

  @Override
  public HashSet<Object> readStrict(JsonParser jp) throws AnoaTypeException, IOException {
    switch (jp.getCurrentToken()) {
      case VALUE_NULL:
        return null;
      case START_ARRAY:
        HashSet<Object> result = new HashSet<>();
        doArray(jp, p -> result.add(elementReader.readStrict(p)));
        return result;
      default:
        throw new AnoaTypeException("Token is not '[': " + jp.getCurrentToken());
    }
  }
}
