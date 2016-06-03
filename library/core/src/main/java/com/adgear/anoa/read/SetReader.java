package com.adgear.anoa.read;

import com.adgear.anoa.AnoaJacksonTypeException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.IOException;
import java.util.HashSet;

class SetReader extends AbstractReader<HashSet<Object>> {

  final AbstractReader<?> elementReader;

  SetReader(AbstractReader<?> elementReader) {
    this.elementReader = elementReader;
  }

  @Override
  protected HashSet<Object> read(JsonParser jacksonParser) throws IOException {
    if (jacksonParser.getCurrentToken() == JsonToken.START_ARRAY) {
      HashSet<Object> result = new HashSet<>();
      doArray(jacksonParser, p -> result.add(elementReader.read(p)));
      return result;
    } else {
      gobbleValue(jacksonParser);
      return null;
    }
  }

  @Override
  protected HashSet<Object> readStrict(JsonParser jacksonParser)
      throws AnoaJacksonTypeException, IOException {
    switch (jacksonParser.getCurrentToken()) {
      case VALUE_NULL:
        return null;
      case START_ARRAY:
        HashSet<Object> result = new HashSet<>();
        doArray(jacksonParser, p -> result.add(elementReader.readStrict(p)));
        return result;
      default:
        throw new AnoaJacksonTypeException("Token is not '[': " + jacksonParser.getCurrentToken());
    }
  }
}
