package com.adgear.anoa.read;

import com.adgear.anoa.AnoaTypeException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.IOException;
import java.util.HashMap;

class MapReader extends JacksonReader<HashMap<String,Object>> {

  final JacksonReader<?> valueElementReader;

  MapReader(JacksonReader<?> valueElementReader) {
    this.valueElementReader = valueElementReader;
  }

  @Override
  public HashMap<String, Object> read(JsonParser jp) throws IOException {
    if (jp.getCurrentToken() == JsonToken.START_OBJECT) {
      HashMap<String, Object> result = new HashMap<>();
      doMap(jp, (k, p) -> result.put(k, valueElementReader.read(p)));
      return result;
    } else {
      gobbleValue(jp);
      return null;
    }
  }

  @Override
  public HashMap<String, Object> readStrict(JsonParser jp) throws AnoaTypeException, IOException {
    switch (jp.getCurrentToken()) {
      case VALUE_NULL:
        return null;
      case START_OBJECT:
        HashMap<String,Object> result = new HashMap<>();
        doMap(jp, (k, p) -> result.put(k, valueElementReader.readStrict(p)));
        return result;
      default:
        throw new AnoaTypeException("Token is not '{': " + jp.getCurrentToken());
    }
  }
}
