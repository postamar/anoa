package com.adgear.anoa.read;

import com.adgear.anoa.AnoaJacksonTypeException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.IOException;
import java.util.HashMap;

class MapReader extends AbstractReader<HashMap<String,Object>> {

  final AbstractReader<?> valueElementReader;

  MapReader(AbstractReader<?> valueElementReader) {
    this.valueElementReader = valueElementReader;
  }

  @Override
  protected HashMap<String, Object> read(JsonParser jacksonParser) throws IOException {
    if (jacksonParser.getCurrentToken() == JsonToken.START_OBJECT) {
      HashMap<String, Object> result = new HashMap<>();
      doMap(jacksonParser, (k, p) -> result.put(k, valueElementReader.read(p)));
      return result;
    } else {
      gobbleValue(jacksonParser);
      return null;
    }
  }

  @Override
  protected HashMap<String, Object> readStrict(JsonParser jacksonParser) throws
                                                                         AnoaJacksonTypeException, IOException {
    switch (jacksonParser.getCurrentToken()) {
      case VALUE_NULL:
        return null;
      case START_OBJECT:
        HashMap<String,Object> result = new HashMap<>();
        doMap(jacksonParser, (k, p) -> result.put(k, valueElementReader.readStrict(p)));
        return result;
      default:
        throw new AnoaJacksonTypeException("Token is not '{': " + jacksonParser.getCurrentToken());
    }
  }
}
